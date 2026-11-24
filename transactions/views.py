import uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path

from django.conf import settings
from django.core.cache import cache
from django.db.models import Avg, Count, F, Sum
from drf_spectacular.types import OpenApiTypes
from drf_spectacular.utils import OpenApiParameter, extend_schema, inline_serializer
from rest_framework import generics, serializers, status
from rest_framework.pagination import PageNumberPagination
from rest_framework.parsers import FormParser, MultiPartParser
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.tokens import RefreshToken

from .cache import ANALYTICS_CACHE_TTL, get_analytics_cache_version
from .models import IngestionJob, TransactionRecord
from .serializers import (
    IngestionJobSerializer,
    TransactionAnalyticsSerializer,
    TransactionRecordSerializer,
    UserRegistrationSerializer,
)
from .tasks import process_csv_file


class TransactionPagination(PageNumberPagination):
    page_size = 50
    page_size_query_param = "page_size"
    max_page_size = 500


class TransactionUploadView(APIView):
    permission_classes = [IsAuthenticated]
    parser_classes = [MultiPartParser, FormParser]

    @extend_schema(
        summary="Upload a CSV file for asynchronous ingestion",
        request={
            "multipart/form-data": {
                "type": "object",
                "properties": {
                    "file": {"type": "string", "format": "binary"},
                },
                "required": ["file"],

            }
        },
        responses={
            202: inline_serializer(
                name="TransactionUploadResponse",
                fields={
                    "message": serializers.CharField(),
                    "job_id": serializers.UUIDField(),
                    "file_name": serializers.CharField(),
                    "status": serializers.CharField(),
                },
            ),
        },
    )
    def post(self, request, *args, **kwargs):
        uploaded_file = request.FILES.get("file")
        if not uploaded_file:
            return Response(
                {"error": "No file provided. Upload a CSV using the 'file' field."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        safe_name = Path(uploaded_file.name).name
        if not safe_name.lower().endswith(".csv"):
            return Response(
                {"error": "Invalid file type. Only .csv files are supported."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        upload_dir = Path(settings.MEDIA_ROOT)
        upload_dir.mkdir(parents=True, exist_ok=True)
        stored_name = f"{uuid.uuid4().hex}_{safe_name}"
        file_path = upload_dir / stored_name

        with file_path.open("wb+") as destination:
            for chunk in uploaded_file.chunks():
                destination.write(chunk)

        job = IngestionJob.objects.create(
            file_name=safe_name,
            source="API_UPLOAD",
            status="PENDING",
        )

        process_csv_file.delay(str(file_path), str(job.job_id))

        return Response(
            {
                "message": "CSV file accepted and queued for background ingestion.",
                "job_id": job.job_id,
                "file_name": job.file_name,
                "status": job.status,
            },
            status=status.HTTP_202_ACCEPTED,
        )


@extend_schema(
    summary="List and filter financial transactions",
    parameters=[
        OpenApiParameter(
            "customer_id",
            OpenApiTypes.STR,
            description="Exact Customer ID (e.g. C5841053)",
        ),
        OpenApiParameter(
            "location",
            OpenApiTypes.STR,
            description="Customer city (e.g. MUMBAI)",
        ),
        OpenApiParameter(
            "gender",
            OpenApiTypes.STR,
            description="Gender code (M / F)",
        ),
        OpenApiParameter(
            "min_amount",
            OpenApiTypes.DECIMAL,
            description="Minimum transaction amount",
        ),
        OpenApiParameter(
            "max_amount",
            OpenApiTypes.DECIMAL,
            description="Maximum transaction amount",
        ),
        OpenApiParameter(
            "start_date",
            OpenApiTypes.DATE,
            description="Start date (YYYY-MM-DD)",
        ),
        OpenApiParameter(
            "end_date",
            OpenApiTypes.DATE,
            description="End date (YYYY-MM-DD)",
        ),
    ],
)
class TransactionListView(generics.ListAPIView):
    serializer_class = TransactionRecordSerializer
    permission_classes = [IsAuthenticated]
    pagination_class = TransactionPagination

    def get_queryset(self):
        queryset = TransactionRecord.objects.all().order_by(
            "-transaction_date",
            "-transaction_time",
        )
        params = self.request.query_params

        customer_id = params.get("customer_id")
        if customer_id:
            queryset = queryset.filter(customer_id=customer_id.strip())

        location = params.get("location")
        if location:
            queryset = queryset.filter(cust_location=location.strip().upper())

        gender = params.get("gender")
        if gender:
            queryset = queryset.filter(cust_gender=gender.strip().upper())

        min_amount = params.get("min_amount")
        if min_amount:
            try:
                queryset = queryset.filter(transaction_amount__gte=Decimal(min_amount))
            except InvalidOperation:
                return TransactionRecord.objects.none()

        max_amount = params.get("max_amount")
        if max_amount:
            try:
                queryset = queryset.filter(transaction_amount__lte=Decimal(max_amount))
            except InvalidOperation:
                return TransactionRecord.objects.none()

        start_date = params.get("start_date")
        if start_date:
            queryset = queryset.filter(transaction_date__gte=start_date)

        end_date = params.get("end_date")
        if end_date:
            queryset = queryset.filter(transaction_date__lte=end_date)

        return queryset


class TransactionAnalyticsView(APIView):
    permission_classes = [IsAuthenticated]

    GROUP_FIELD_MAP = {
        "location": "cust_location",
        "gender": "cust_gender",
    }

    @extend_schema(
        summary="Aggregate transaction volume and average balance",
        parameters=[
            OpenApiParameter(
                name="group_by",
                type=OpenApiTypes.STR,
                enum=["location", "gender"],
                default="location",
                description="Dimension to group analytics by (location or gender).",
            ),
            OpenApiParameter(
                name="start_date",
                type=OpenApiTypes.DATE,
                description="Optional start date filter (YYYY-MM-DD).",
            ),
            OpenApiParameter(
                name="end_date",
                type=OpenApiTypes.DATE,
                description="Optional end date filter (YYYY-MM-DD).",
            ),
            OpenApiParameter(
                name="limit",
                type=OpenApiTypes.INT,
                default=20,
                description="Max number of top groups to return (ordered by total volume).",
            ),
        ],
        responses={
            200: inline_serializer(
                name="TransactionAnalyticsEnvelope",
                fields={
                    "group_by": serializers.CharField(),
                    "results": TransactionAnalyticsSerializer(many=True),
                },
            ),
        },
    )
    def get(self, request, *args, **kwargs):
        group_key = request.query_params.get("group_by", "location").lower()
        if group_key not in self.GROUP_FIELD_MAP:
            return Response(
                {"error": "Invalid group_by parameter. Use 'location' or 'gender'."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        start_date = request.query_params.get("start_date") or ""
        end_date = request.query_params.get("end_date") or ""
        try:
            limit = max(1, min(int(request.query_params.get("limit", 20)), 100))
        except ValueError:
            limit = 20

        version = get_analytics_cache_version()
        cache_key = (
            f"analytics:v{version}:{group_key}:"
            f"{start_date or 'all'}:{end_date or 'all'}:{limit}"
        )

        cached_payload = cache.get(cache_key)
        if cached_payload is not None:
            return Response(cached_payload, headers={"X-Cache": "HIT"})

        db_field = self.GROUP_FIELD_MAP[group_key]
        queryset = TransactionRecord.objects.exclude(
            **{f"{db_field}__isnull": True}
        ).exclude(**{db_field: ""})

        if start_date:
            queryset = queryset.filter(transaction_date__gte=start_date)
        if end_date:
            queryset = queryset.filter(transaction_date__lte=end_date)

        results = (
            queryset.values(group=F(db_field))
            .annotate(
                transaction_count=Count("transaction_id"),
                total_transaction_volume=Sum("transaction_amount"),
                avg_account_balance=Avg("cust_account_balance"),
            )
            .order_by("-total_transaction_volume")[:limit]
        )

        serializer = TransactionAnalyticsSerializer(results, many=True)
        payload = {
            "group_by": group_key,
            "results": [dict(row) for row in serializer.data],
        }

        cache.set(cache_key, payload, timeout=ANALYTICS_CACHE_TTL)
        return Response(payload, headers={"X-Cache": "MISS"})


@extend_schema(summary="List all CSV ingestion audit jobs")
class IngestionJobListView(generics.ListAPIView):
    queryset = IngestionJob.objects.all()
    serializer_class = IngestionJobSerializer
    permission_classes = [IsAuthenticated]
    pagination_class = TransactionPagination


@extend_schema(summary="Retrieve details for a single ingestion job")
class IngestionJobDetailView(generics.RetrieveAPIView):
    queryset = IngestionJob.objects.all()
    serializer_class = IngestionJobSerializer
    permission_classes = [IsAuthenticated]
    lookup_field = "job_id"



class UserRegistrationView(generics.CreateAPIView):
    """Public signup endpoint that creates a user and returns a fresh JWT pair."""

    serializer_class = UserRegistrationSerializer
    authentication_classes = []
    permission_classes = [AllowAny]

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.save()

        refresh = RefreshToken.for_user(user)
        return Response(
            {
                "user": {
                    "id": user.id,
                    "username": user.username,
                    "email": user.email,
                },
                "access": str(refresh.access_token),
                "refresh": str(refresh),
            },
            status=status.HTTP_201_CREATED,
        )