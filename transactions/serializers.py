from django.contrib.auth import get_user_model
from django.contrib.auth.password_validation import validate_password
from rest_framework import serializers

from .models import IngestionJob, TransactionRecord


class TransactionRecordSerializer(serializers.ModelSerializer):
    class Meta:
        model = TransactionRecord
        fields = [
            "transaction_id",
            "customer_id",
            "customer_dob",
            "cust_gender",
            "cust_location",
            "cust_account_balance",
            "transaction_date",
            "transaction_time",
            "transaction_amount",
        ]


class IngestionJobSerializer(serializers.ModelSerializer):
    class Meta:
        model = IngestionJob
        fields = [
            "job_id",
            "file_name",
            "source",
            "status",
            "total_rows",
            "processing_time_seconds",
            "error_message",
            "created_at",
            "completed_at",
        ]


class TransactionAnalyticsSerializer(serializers.Serializer):
    group = serializers.CharField()
    transaction_count = serializers.IntegerField()
    total_transaction_volume = serializers.DecimalField(
        max_digits=18,
        decimal_places=2,
        allow_null=True,
    )
    avg_account_balance = serializers.DecimalField(
        max_digits=15,
        decimal_places=2,
        allow_null=True,
    )

User = get_user_model()


class UserRegistrationSerializer(serializers.ModelSerializer):
    """Validates and creates a new user account with a hashed password."""

    password = serializers.CharField(
        write_only=True,
        required=True,
        validators=[validate_password],
        style={"input_type": "password"},
    )
    password_confirm = serializers.CharField(
        write_only=True,
        required=True,
        style={"input_type": "password"},
    )

    class Meta:
        model = User
        fields = ("id", "username", "email", "password", "password_confirm")
        read_only_fields = ("id",)

    def validate(self, attrs):
        if attrs["password"] != attrs["password_confirm"]:
            raise serializers.ValidationError(
                {"password_confirm": "Passwords do not match."}
            )
        return attrs

    def create(self, validated_data):
        validated_data.pop("password_confirm")
        return User.objects.create_user(
            username=validated_data["username"],
            email=validated_data.get("email", ""),
            password=validated_data["password"],
        )