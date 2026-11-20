import os
import uuid
from django.conf import settings
from rest_framework import status, generics
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.pagination import PageNumberPagination

from .models import TransactionRecord, IngestionJob
from .serializers import TransactionRecordSerializer, IngestionJobSerializer
from .tasks import process_financial_csv


class TransactionPagination(PageNumberPagination):
    page_size = 50
    page_size_query_param = "page_size"
    max_page_size = 500


class TransactionUploadView(APIView):
    permission_classes = [IsAuthenticated]
    parser_classes = [MultiPartParser, FormParser]

    def post(self, request, *args, **kwargs):
        uploaded_file = request.FILES.get("file")
        if not uploaded_file:
            return Response(
                {"error": 'No file provided. Attach a CSV under the key "file".'},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not uploaded_file.name.endswith(".csv"):
            return Response(
                {"error": "Invalid file format. Only .csv files are supported."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        upload_dir = os.path.join(settings.BASE_DIR, "uploads")
        os.makedirs(upload_dir, exist_ok=True)

        unique_filename = f"{uuid.uuid4().hex}_{uploaded_file.name}"
        file_path = os.path.join(upload_dir, unique_filename)

        with open(file_path, "wb+") as destination:
            for chunk in uploaded_file.chunks():
                destination.write(chunk)

        job = IngestionJob.objects.create(
            file_name=uploaded_file.name,
            source="API_UPLOAD",
            status="PENDING",
        )
        task = process_financial_csv.delay(file_path, str(job.job_id))

        return Response(
            {
                "message": "CSV uploaded and queued for background processing.",
                "job_id": str(job.job_id),
                "task_id": task.id,
                "file_name": uploaded_file.name,
            },
            status=status.HTTP_202_ACCEPTED,
        )


class TransactionListView(generics.ListAPIView):
    serializer_class = TransactionRecordSerializer
    permission_classes = [IsAuthenticated]
    pagination_class = TransactionPagination

    def get_queryset(self):
        queryset = TransactionRecord.objects.all().order_by(
            "-transaction_date", "-transaction_id"
        )
        params = self.request.query_params

        customer_id = params.get("customer_id")
        if customer_id:
            queryset = queryset.filter(customer_id=customer_id)

        location = params.get("location")
        if location:
            queryset = queryset.filter(cust_location=location.upper())

        gender = params.get("gender")
        if gender:
            queryset = queryset.filter(cust_gender=gender.upper())

        min_amount = params.get("min_amount")
        if min_amount:
            queryset = queryset.filter(transaction_amount__gte=min_amount)

        max_amount = params.get("max_amount")
        if max_amount:
            queryset = queryset.filter(transaction_amount__lte=max_amount)

        start_date = params.get("start_date")
        if start_date:
            queryset = queryset.filter(transaction_date__gte=start_date)

        end_date = params.get("end_date")
        if end_date:
            queryset = queryset.filter(transaction_date__lte=end_date)

        return queryset


class IngestionJobListView(generics.ListAPIView):
    queryset = IngestionJob.objects.all()
    serializer_class = IngestionJobSerializer
    permission_classes = [IsAuthenticated]
    pagination_class = TransactionPagination


class IngestionJobDetailView(generics.RetrieveAPIView):
    queryset = IngestionJob.objects.all()
    serializer_class = IngestionJobSerializer
    permission_classes = [IsAuthenticated]
    lookup_field = "job_id"