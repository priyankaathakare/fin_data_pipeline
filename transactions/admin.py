import shutil
from pathlib import Path

from django.conf import settings
from django.contrib import admin, messages
from django.utils.html import format_html

from .models import IngestionJob, TransactionRecord
from .tasks import process_csv_file


def _find_latest_matching_file(directory: Path, file_name: str) -> Path | None:
    """Locate the most recent <uuid>_<file_name> or exact <file_name> match."""
    if not directory.exists():
        return None

    candidates = [
        path
        for path in (*directory.glob(f"*_{file_name}"), directory / file_name)
        if path.is_file()
    ]
    if not candidates:
        return None

    return max(candidates, key=lambda path: path.stat().st_mtime)


def _stage_file_for_retry(job: IngestionJob) -> Path | None:
    """
    Resolve the failed CSV on disk and move AUTO_DROP files back into
    data_drop/processing/ so Celery Beat cannot double-queue them.
    """
    if job.source == "AUTO_DROP":
        drop_dir = Path(
            getattr(settings, "DATA_DROP_DIR", settings.BASE_DIR / "data_drop")
        )
        failed_file = _find_latest_matching_file(drop_dir / "failed", job.file_name)
        if failed_file is None:
            return None

        processing_dir = drop_dir / "processing"
        processing_dir.mkdir(parents=True, exist_ok=True)
        staged_path = processing_dir / failed_file.name
        shutil.move(str(failed_file), str(staged_path))
        return staged_path

    upload_dir = Path(settings.MEDIA_ROOT)
    return _find_latest_matching_file(upload_dir, job.file_name)


@admin.register(IngestionJob)
class IngestionJobAdmin(admin.ModelAdmin):
    list_display = (
        "short_job_id",
        "file_name",
        "source",
        "colored_status",
        "total_rows",
        "processing_time_seconds",
        "created_at",
        "completed_at",
    )
    list_filter = ("status", "source", "created_at")
    search_fields = ("job_id", "file_name", "error_message")
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
    ordering = ("-created_at",)
    actions = ["retry_failed_jobs"]

    STATUS_COLORS = {
        "PENDING": "#d97706",
        "PROCESSING": "#2563eb",
        "COMPLETED": "#16a34a",
        "FAILED": "#dc2626",
    }

    @admin.display(description="Job ID")
    def short_job_id(self, obj):
        return str(obj.job_id)[:8]

    @admin.display(description="Status", ordering="status")
    def colored_status(self, obj):
        color = self.STATUS_COLORS.get(obj.status, "#6b7280")
        return format_html(
            '<span style="background:{}; color:#fff; padding:3px 8px; '
            'border-radius:12px; font-weight:600; font-size:11px;">{}</span>',
            color,
            obj.status,
        )

    @admin.action(description="Retry selected FAILED ingestion jobs")
    def retry_failed_jobs(self, request, queryset):
        requeued = 0
        skipped_status = 0
        missing_files = []

        for job in queryset:
            if job.status != "FAILED":
                skipped_status += 1
                continue

            retry_path = _stage_file_for_retry(job)
            if retry_path is None:
                missing_files.append(f"{job.file_name} ({str(job.job_id)[:8]})")
                continue

            job.status = "PENDING"
            job.error_message = None
            job.processing_time_seconds = None
            job.completed_at = None
            job.save(
                update_fields=[
                    "status",
                    "error_message",
                    "processing_time_seconds",
                    "completed_at",
                ]
            )

            process_csv_file.delay(str(retry_path), str(job.job_id))
            requeued += 1

        if requeued:
            self.message_user(
                request,
                f"Re-queued {requeued} failed ingestion job(s) for processing.",
                level=messages.SUCCESS,
            )
        if skipped_status:
            self.message_user(
                request,
                f"Skipped {skipped_status} job(s) because only FAILED jobs can be retried.",
                level=messages.WARNING,
            )
        if missing_files:
            self.message_user(
                request,
                f"Source CSV not found on disk for: {', '.join(missing_files)}.",
                level=messages.ERROR,
            )


@admin.register(TransactionRecord)
class TransactionRecordAdmin(admin.ModelAdmin):
    list_display = (
        "transaction_id",
        "customer_id",
        "cust_location",
        "cust_gender",
        "transaction_amount",
        "transaction_date",
    )
    list_filter = ("cust_gender", "transaction_date")
    search_fields = ("transaction_id", "customer_id", "cust_location")
    list_per_page = 50