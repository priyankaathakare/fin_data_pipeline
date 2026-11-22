import uuid

from django.db import models


class TransactionRecord(models.Model):
    transaction_id = models.CharField(max_length=50, primary_key=True)
    customer_id = models.CharField(max_length=50)
    customer_dob = models.CharField(max_length=20, null=True, blank=True)
    cust_gender = models.CharField(max_length=10, null=True, blank=True)
    cust_location = models.CharField(max_length=100, null=True, blank=True)
    cust_account_balance = models.DecimalField(
        max_digits=15, decimal_places=2, null=True, blank=True
    )
    transaction_date = models.DateField(null=True, blank=True)
    transaction_time = models.CharField(max_length=20, null=True, blank=True)
    transaction_amount = models.DecimalField(
        max_digits=15, decimal_places=2, null=True, blank=True
    )

    class Meta:
        indexes = [
            models.Index(fields=["customer_id", "-transaction_date"], name="idx_cust_date"),
            models.Index(fields=["transaction_date"], name="idx_tx_date"),
            models.Index(fields=["transaction_amount"], name="idx_tx_amount"),
            models.Index(fields=["cust_location"], name="idx_cust_location"),
        ]

    def __str__(self):
        return f"{self.transaction_id} - {self.customer_id}"


class IngestionJob(models.Model):
    STATUS_CHOICES = [
        ("PENDING", "Pending"),
        ("PROCESSING", "Processing"),
        ("COMPLETED", "Completed"),
        ("FAILED", "Failed"),
    ]
    SOURCE_CHOICES = [
        ("AUTO_DROP", "Automated Drop Zone"),
        ("API_UPLOAD", "API Upload"),
    ]

    job_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    file_name = models.CharField(max_length=255)
    source = models.CharField(max_length=20, choices=SOURCE_CHOICES, default="AUTO_DROP")
    status = models.CharField(
        max_length=20, choices=STATUS_CHOICES, default="PENDING", db_index=True
    )
    total_rows = models.IntegerField(default=0)
    processing_time_seconds = models.FloatField(null=True, blank=True)
    error_message = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.file_name} ({self.status})"