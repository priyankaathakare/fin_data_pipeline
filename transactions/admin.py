from django.contrib import admin

from .models import IngestionJob, TransactionRecord


@admin.register(TransactionRecord)
class TransactionRecordAdmin(admin.ModelAdmin):
    list_display = (
        "transaction_id",
        "customer_id",
        "transaction_date",
        "transaction_amount",
        "cust_location",
        "cust_gender",
    )
    list_filter = ("cust_gender", "transaction_date")
    search_fields = ("transaction_id", "customer_id", "cust_location")
    ordering = ("-transaction_date", "-transaction_id")
    list_per_page = 50
    # Avoids running a slow full-table COUNT(*) on 1M+ rows on every admin page load
    show_full_result_count = False


@admin.register(IngestionJob)
class IngestionJobAdmin(admin.ModelAdmin):
    list_display = (
        "file_name",
        "source",
        "status",
        "total_rows",
        "processing_time_seconds",
        "created_at",
        "completed_at",
    )
    list_filter = ("status", "source", "created_at")
    search_fields = ("job_id", "file_name", "error_message")
    ordering = ("-created_at",)
    list_per_page = 25
    readonly_fields = (
        "job_id",
        "file_name",
        "source",
        "status",
        "total_rows",
        "processing_time_seconds",
        "error_message",
        "created_at",
        "completed_at",
    )