import csv
import io
import logging
import shutil
import time
import uuid
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from celery import shared_task
from django.conf import settings
from django.db import OperationalError, connection, transaction
from django.utils import timezone

from .cache import invalidate_analytics_cache
from .models import IngestionJob, TransactionRecord

logger = logging.getLogger(__name__)

CHUNK_SIZE = 50_000

COPY_COLUMNS = (
    "transaction_id",
    "customer_id",
    "customer_dob",
    "cust_gender",
    "cust_location",
    "cust_account_balance",
    "transaction_date",
    "transaction_time",
    "transaction_amount",
)

DATE_FORMATS = (
    "%Y-%m-%d",
    "%d/%m/%y",
    "%d/%m/%Y",
    "%m/%d/%y",
    "%m/%d/%Y",
    "%d-%m-%Y",
)


def _get_field(row: dict[str, str], *keys: str) -> str:
    """Return the first non-empty stripped value across candidate CSV headers."""
    for key in keys:
        value = row.get(key)
        if value is not None:
            cleaned = value.strip()
            if cleaned and cleaned.lower() != "nan":
                return cleaned
    return ""


def _parse_date(value: str) -> str:
    """Normalize supported date strings to ISO YYYY-MM-DD for PostgreSQL COPY."""
    if not value:
        return ""
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def _parse_decimal(value: str) -> str:
    """Validate decimal values; return empty string (NULL) for malformed entries."""
    if not value:
        return ""
    try:
        return str(Decimal(value))
    except InvalidOperation:
        return ""


def _archive_drop_zone_file(csv_path: Path, subfolder: str) -> None:
    """Move drop-zone files out of processing/ into processed/ or failed/."""
    if csv_path.parent.name != "processing" or not csv_path.exists():
        return
    archive_dir = csv_path.parent.parent / subfolder
    archive_dir.mkdir(parents=True, exist_ok=True)
    shutil.move(str(csv_path), str(archive_dir / csv_path.name))


@shared_task(bind=True, max_retries=3)
def process_csv_file(self, file_path: str, job_id: str | None = None) -> dict:
    """
    Bulk-load a CSV file into PostgreSQL via a TEMP staging table + COPY,
    upsert idempotently into TransactionRecord, update IngestionJob metrics,
    and invalidate cached analytics responses.
    """
    csv_path = Path(file_path)
    start_time = time.perf_counter()

    if job_id:
        job = IngestionJob.objects.get(job_id=job_id)
    else:
        job = IngestionJob.objects.create(
            file_name=csv_path.name,
            source="AUTO_DROP",
            status="PENDING",
        )

    job.status = "PROCESSING"
    job.error_message = None
    job.save(update_fields=["status", "error_message"])

    target_table = TransactionRecord._meta.db_table
    staging_table = f"tmp_tx_staging_{job.job_id.hex[:12]}"
    col_list = ", ".join(COPY_COLUMNS)
    copy_sql = f"COPY {staging_table} ({col_list}) FROM STDIN WITH (FORMAT CSV, NULL '');"

    total_rows = 0
    inserted_rows = 0

    try:
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        with (
            csv_path.open("r", encoding="utf-8-sig", newline="") as csv_file,
            transaction.atomic(),
            connection.cursor() as cursor,
        ):
            # Omit constraints on temp table so intra-file duplicates don't abort COPY
            cursor.execute(
                f"CREATE TEMP TABLE {staging_table} "
                f"(LIKE {target_table} INCLUDING DEFAULTS) ON COMMIT DROP;"
            )

            reader = csv.DictReader(csv_file)
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            chunk_count = 0

            for row in reader:
                tx_id = _get_field(row, "TransactionID", "transaction_id")
                cust_id = _get_field(row, "CustomerID", "customer_id")
                if not tx_id or not cust_id:
                    continue

                writer.writerow(
                    [
                        tx_id,
                        cust_id,
                        _get_field(row, "CustomerDOB", "customer_dob"),
                        _get_field(row, "CustGender", "cust_gender").upper(),
                        _get_field(row, "CustLocation", "cust_location").upper(),
                        _parse_decimal(
                            _get_field(row, "CustAccountBalance", "cust_account_balance")
                        ),
                        _parse_date(
                            _get_field(row, "TransactionDate", "transaction_date")
                        ),
                        _get_field(row, "TransactionTime", "transaction_time"),
                        _parse_decimal(
                            _get_field(
                                row,
                                "TransactionAmount (INR)",
                                "TransactionAmount",
                                "transaction_amount",
                            )
                        ),
                    ]
                )
                total_rows += 1
                chunk_count += 1

                if chunk_count >= CHUNK_SIZE:
                    buffer.seek(0)
                    cursor.copy_expert(copy_sql, buffer)
                    buffer = io.StringIO()
                    writer = csv.writer(buffer)
                    chunk_count = 0

            if chunk_count > 0:
                buffer.seek(0)
                cursor.copy_expert(copy_sql, buffer)

            cursor.execute(
                f"INSERT INTO {target_table} ({col_list}) "
                f"SELECT DISTINCT ON (transaction_id) {col_list} "
                f"FROM {staging_table} "
                f"ON CONFLICT (transaction_id) DO NOTHING;"
            )
            inserted_rows = cursor.rowcount

        elapsed = round(time.perf_counter() - start_time, 3)
        job.status = "COMPLETED"
        job.total_rows = total_rows
        job.processing_time_seconds = elapsed
        job.completed_at = timezone.now()
        job.save(
            update_fields=[
                "status",
                "total_rows",
                "processing_time_seconds",
                "completed_at",
            ]
        )

        # Invalidate Redis analytics cache so fresh totals are served immediately
        invalidate_analytics_cache()
        _archive_drop_zone_file(csv_path, "processed")

        logger.info(
            "IngestionJob %s completed: %s rows staged, %s new rows inserted in %ss.",
            job.job_id,
            total_rows,
            inserted_rows,
            elapsed,
        )
        return {
            "job_id": str(job.job_id),
            "status": job.status,
            "total_rows": total_rows,
            "inserted_rows": inserted_rows,
            "processing_time_seconds": elapsed,
        }

    except OperationalError as exc:
        if self.request.retries < self.max_retries:
            countdown = 2**self.request.retries * 5
            logger.warning(
                "Transient DB error on job %s; retrying in %ss (%s/%s): %s",
                job.job_id,
                countdown,
                self.request.retries + 1,
                self.max_retries,
                exc,
            )
            raise self.retry(exc=exc, countdown=countdown) from exc

        elapsed = round(time.perf_counter() - start_time, 3)
        job.status = "FAILED"
        job.processing_time_seconds = elapsed
        job.error_message = str(exc)
        job.completed_at = timezone.now()
        job.save(
            update_fields=[
                "status",
                "processing_time_seconds",
                "error_message",
                "completed_at",
            ]
        )
        _archive_drop_zone_file(csv_path, "failed")
        logger.exception("IngestionJob %s failed after max retries.", job.job_id)
        raise

    except Exception as exc:
        elapsed = round(time.perf_counter() - start_time, 3)
        job.status = "FAILED"
        job.processing_time_seconds = elapsed
        job.error_message = str(exc)
        job.completed_at = timezone.now()
        job.save(
            update_fields=[
                "status",
                "processing_time_seconds",
                "error_message",
                "completed_at",
            ]
        )
        _archive_drop_zone_file(csv_path, "failed")
        logger.exception("IngestionJob %s failed.", job.job_id)
        raise


@shared_task
def scan_drop_zone() -> dict:
    """
    Celery Beat task: detect new CSV files in DATA_DROP_DIR, move them atomically
    to processing/ to prevent double-queuing, and dispatch process_csv_file jobs.
    """
    drop_dir = Path(getattr(settings, "DATA_DROP_DIR", settings.BASE_DIR / "data_drop"))
    processing_dir = drop_dir / "processing"
    drop_dir.mkdir(parents=True, exist_ok=True)
    processing_dir.mkdir(parents=True, exist_ok=True)

    queued_jobs = []
    for csv_file in sorted(drop_dir.glob("*.csv")):
        if not csv_file.is_file():
            continue

        staged_name = f"{uuid.uuid4().hex}_{csv_file.name}"
        staged_path = processing_dir / staged_name
        shutil.move(str(csv_file), str(staged_path))

        job = IngestionJob.objects.create(
            file_name=csv_file.name,
            source="AUTO_DROP",
            status="PENDING",
        )
        process_csv_file.delay(str(staged_path), str(job.job_id))
        queued_jobs.append(str(job.job_id))
        logger.info("Queued drop-zone file %s as IngestionJob %s.", csv_file.name, job.job_id)

    return {"queued_count": len(queued_jobs), "job_ids": queued_jobs}