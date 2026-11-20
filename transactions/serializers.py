from rest_framework import serializers
from .models import TransactionRecord, IngestionJob


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