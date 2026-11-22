import io
import os
import shutil
import time

import pandas as pd
from celery import shared_task
from django.conf import settings
from django.db import connection, transaction
from django.utils import timezone

from .models import IngestionJob, TransactionRecord

BASE_DROP_DIR = os.path.join(settings.BASE_DIR, "data_drop")
INCOMING_DIR = os.path.join(BASE_DROP_DIR, "incoming")
PROCESSING_DIR = os.path.join(BASE_DROP_DIR, "processing")
PROCESSED_DIR = os.path.join(BASE_DROP_DIR, "processed")
FAILED_DIR = os.path.join(BASE_DROP_DIR, "failed")


def _ensure_drop_dirs():
    for path in (INCOMING_DIR, PROCESSING_DIR, PROCESSED_DIR, FAILED_DIR):
        os.makedirs(path, exist_ok=True)


@shared_task
def scan_incoming_folder():
    """Scheduled task: scans incoming/ for CSVs and dispatches tracked ingestion jobs."""
    _ensure_drop_dirs()
    dispatched = []

    for filename in os.listdir(INCOMING_DIR):
        if not filename.lower().endswith(".csv"):
            continue

        src_path = os.path.join(INCOMING_DIR, filename)
        if not os.path.isfile(src_path):
            continue

        processing_path = os.path.join(PROCESSING_DIR, filename)
        shutil.move(src_path, processing_path)

        job = IngestionJob.objects.create(
            file_name=filename,
            source="AUTO_DROP",
            status="PENDING",
        )
        process_financial_csv.delay(processing_path, str(job.job_id))
        dispatched.append(f"{filename} ({job.job_id})")

    if dispatched:
        return f"Dispatched {len(dispatched)} job(s): {', '.join(dispatched)}"
    return "No new CSV files found."


@shared_task
def process_financial_csv(file_path, job_id=None):
    _ensure_drop_dirs()
    start_time = time.time()

    job = IngestionJob.objects.filter(job_id=job_id).first() if job_id else None
    if job:
        job.status = "PROCESSING"
        job.save(update_fields=["status"])

    target_table = TransactionRecord._meta.db_table
    staging_table = "staging_transactions"

    db_columns = [
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
    col_list = ", ".join(db_columns)

    csv_to_db_map = {
        "TransactionID": "transaction_id",
        "CustomerID": "customer_id",
        "CustomerDOB": "customer_dob",
        "CustGender": "cust_gender",
        "CustLocation": "cust_location",
        "CustAccountBalance": "cust_account_balance",
        "TransactionDate": "transaction_date",
        "TransactionTime": "transaction_time",
        "TransactionAmount (INR)": "transaction_amount",
    }

    total_processed = 0
    is_drop_zone_file = (
        os.path.dirname(os.path.abspath(file_path)) == os.path.abspath(PROCESSING_DIR)
    )

    try:
        with transaction.atomic(), connection.cursor() as cursor:
                cursor.execute(
                    f"CREATE TEMP TABLE {staging_table} (LIKE {target_table}) ON COMMIT DROP;"
                )

                for chunk in pd.read_csv(file_path, chunksize=50000):
                    chunk = chunk.rename(columns=csv_to_db_map)[db_columns]
                    chunk["transaction_date"] = pd.to_datetime(
                        chunk["transaction_date"], format="%d/%m/%y", errors="coerce"
                    ).dt.strftime("%Y-%m-%d")

                    buffer = io.StringIO()
                    chunk.to_csv(buffer, index=False, header=False, na_rep="")
                    buffer.seek(0)

                    copy_sql = (
                        f"COPY {staging_table} ({col_list}) "
                        f"FROM STDIN WITH (FORMAT CSV, NULL '');"
                    )
                    cursor.copy_expert(copy_sql, buffer)

                    cursor.execute(
                        f"""
                        INSERT INTO {target_table} ({col_list})
                        SELECT {col_list} FROM {staging_table}
                        ON CONFLICT (transaction_id) DO NOTHING;
                        """
                    )
                    cursor.execute(f"TRUNCATE TABLE {staging_table};")
                    total_processed += len(chunk)

        if is_drop_zone_file:
            shutil.move(
                file_path, os.path.join(PROCESSED_DIR, os.path.basename(file_path))
            )

        elapsed = round(time.time() - start_time, 2)

        if job:
            job.status = "COMPLETED"
            job.total_rows = total_processed
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

        return f"Processed {total_processed} rows via PostgreSQL COPY in {elapsed}s"

    except Exception as exc:
        if is_drop_zone_file and os.path.exists(file_path):
            shutil.move(
                file_path, os.path.join(FAILED_DIR, os.path.basename(file_path))
            )

        if job:
            job.status = "FAILED"
            job.error_message = str(exc)
            job.processing_time_seconds = round(time.time() - start_time, 2)
            job.completed_at = timezone.now()
            job.save(
                update_fields=[
                    "status",
                    "error_message",
                    "processing_time_seconds",
                    "completed_at",
                ]
            )
        raise