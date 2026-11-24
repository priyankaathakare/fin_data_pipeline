import csv
import tempfile
from decimal import Decimal
from pathlib import Path

from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.test import TransactionTestCase, override_settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITransactionTestCase

from .models import IngestionJob, TransactionRecord
from .tasks import process_csv_file

CSV_HEADERS = [
    "TransactionID",
    "CustomerID",
    "CustomerDOB",
    "CustGender",
    "CustLocation",
    "CustAccountBalance",
    "TransactionDate",
    "TransactionTime",
    "TransactionAmount (INR)",
]


def _write_csv(file_path: Path, rows: list[list[str]]) -> Path:
    """Write a clean UTF-8 CSV file with standard banking transaction headers."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(CSV_HEADERS)
        writer.writerows(rows)
    return file_path


class CSVIngestionIdempotencyTests(TransactionTestCase):
    """Verify atomic PostgreSQL COPY ingestion and strict ledger idempotency."""

    def setUp(self):
        cache.clear()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.drop_dir = Path(self.temp_dir.name)
        self.processing_dir = self.drop_dir / "processing"
        self.processed_dir = self.drop_dir / "processed"
        self.failed_dir = self.drop_dir / "failed"
        self.processing_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        cache.clear()
        self.temp_dir.cleanup()

    def test_ingestion_is_idempotent_across_intra_file_and_repeat_batches(self):
        """
        Duplicate TransactionIDs within the same CSV or re-sent in a later batch
        must never create duplicate ledger rows or fail the ingestion job.
        """
        with override_settings(DATA_DROP_DIR=self.drop_dir):
            # Batch 1: 3 rows containing an intra-file duplicate (TXN_BANK_001)
            batch_1 = _write_csv(
                self.processing_dir / "batch_1.csv",
                [
                    [
                        "TXN_BANK_001",
                        "CUST1001",
                        "15/08/94",
                        "F",
                        "PUNE",
                        "150000.00",
                        "23/11/26",
                        "101500",
                        "5000.00",
                    ],
                    [
                        "TXN_BANK_001",
                        "CUST1001",
                        "15/08/94",
                        "F",
                        "PUNE",
                        "150000.00",
                        "23/11/26",
                        "101500",
                        "5000.00",
                    ],
                    [
                        "TXN_BANK_002",
                        "CUST1002",
                        "02/04/91",
                        "M",
                        "MUMBAI",
                        "80000.00",
                        "23/11/26",
                        "102000",
                        "12500.50",
                    ],
                ],
            )
            job_1 = IngestionJob.objects.create(
                file_name="batch_1.csv",
                source="AUTO_DROP",
                status="PENDING",
            )

            process_csv_file(str(batch_1), str(job_1.job_id))
            job_1.refresh_from_db()

            self.assertEqual(job_1.status, "COMPLETED")
            self.assertIsNone(job_1.error_message)
            self.assertEqual(TransactionRecord.objects.count(), 2)
            self.assertFalse(batch_1.exists())
            self.assertTrue((self.processed_dir / "batch_1.csv").is_file())

            # Batch 2: replays TXN_BANK_002 alongside one brand-new TXN_BANK_003
            batch_2 = _write_csv(
                self.processing_dir / "batch_2.csv",
                [
                    [
                        "TXN_BANK_002",
                        "CUST1002",
                        "02/04/91",
                        "M",
                        "MUMBAI",
                        "80000.00",
                        "23/11/26",
                        "102000",
                        "12500.50",
                    ],
                    [
                        "TXN_BANK_003",
                        "CUST1003",
                        "10/11/89",
                        "F",
                        "DELHI",
                        "210000.00",
                        "23/11/26",
                        "103000",
                        "7500.00",
                    ],
                ],
            )
            job_2 = IngestionJob.objects.create(
                file_name="batch_2.csv",
                source="AUTO_DROP",
                status="PENDING",
            )

            process_csv_file(str(batch_2), str(job_2.job_id))
            job_2.refresh_from_db()

            self.assertEqual(job_2.status, "COMPLETED")
            self.assertEqual(TransactionRecord.objects.count(), 3)
            self.assertTrue((self.processed_dir / "batch_2.csv").is_file())


class DeadLetterQuarantineTests(TransactionTestCase):
    """Verify malformed files fail fast, roll back cleanly, and move to failed/."""

    def setUp(self):
        cache.clear()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.drop_dir = Path(self.temp_dir.name)
        self.processing_dir = self.drop_dir / "processing"
        self.failed_dir = self.drop_dir / "failed"
        self.processing_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        cache.clear()
        self.temp_dir.cleanup()

    def test_corrupt_utf8_csv_is_quarantined_and_logged_as_failed(self):
        """
        A non-UTF-8 file must raise UnicodeDecodeError, leave zero partial rows
        in PostgreSQL, mark IngestionJob as FAILED, and move to data_drop/failed/.
        """
        with override_settings(DATA_DROP_DIR=self.drop_dir):
            corrupt_path = self.processing_dir / "corrupt_ledger.csv"
            corrupt_path.write_bytes(b"\xff\xfe\x80\x81")

            job = IngestionJob.objects.create(
                file_name="corrupt_ledger.csv",
                source="AUTO_DROP",
                status="PENDING",
            )

            with self.assertRaises(UnicodeDecodeError):
                process_csv_file(str(corrupt_path), str(job.job_id))

            job.refresh_from_db()
            self.assertEqual(job.status, "FAILED")
            self.assertIsNotNone(job.completed_at)
            self.assertIn("utf-8", (job.error_message or "").lower())
            self.assertEqual(TransactionRecord.objects.count(), 0)
            self.assertFalse(corrupt_path.exists())
            self.assertTrue((self.failed_dir / "corrupt_ledger.csv").is_file())


class AnalyticsCacheInvalidationTests(APITransactionTestCase):
    """Verify Redis analytics caching (X-Cache: MISS/HIT) and post-ingest invalidation."""

    def setUp(self):
        cache.clear()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.drop_dir = Path(self.temp_dir.name)
        self.processing_dir = self.drop_dir / "processing"
        self.processing_dir.mkdir(parents=True, exist_ok=True)

        user_model = get_user_model()
        self.user = user_model.objects.create_user(
            username="bank_auditor",
            password="StrongTestPassword123!",
        )
        self.client.force_authenticate(user=self.user)

    def tearDown(self):
        cache.clear()
        self.temp_dir.cleanup()

    @staticmethod
    def _extract_rows(payload):
        if isinstance(payload, list):
            return payload
        for key in ("results", "data", "analytics"):
            if key in payload and isinstance(payload[key], list):
                return payload[key]
        raise AssertionError(f"Unexpected analytics payload shape: {payload}")

    def test_analytics_cache_miss_hit_and_automatic_invalidation(self):
        """
        1st analytics call -> MISS (populates Redis DB 1).
        2nd analytics call -> HIT (served from Redis).
        New CSV ingestion -> bumps cache version -> next call is MISS with fresh totals.
        """
        url = "/api/transactions/analytics/?group_by=location"

        with override_settings(DATA_DROP_DIR=self.drop_dir):
            # Ingest initial batch (PUNE total = 1000.00)
            batch_1 = _write_csv(
                self.processing_dir / "analytics_batch_1.csv",
                [
                    [
                        "TXN_CACHE_001",
                        "CUST2001",
                        "12/06/93",
                        "F",
                        "PUNE",
                        "100000.00",
                        "23/11/26",
                        "110000",
                        "1000.00",
                    ],
                ],
            )
            job_1 = IngestionJob.objects.create(
                file_name="analytics_batch_1.csv",
                source="AUTO_DROP",
                status="PENDING",
            )
            process_csv_file(str(batch_1), str(job_1.job_id))

            # 1st call: cold cache -> MISS
            resp_1 = self.client.get(url)
            self.assertEqual(resp_1.status_code, status.HTTP_200_OK)
            self.assertEqual(resp_1["X-Cache"], "MISS")
            pune_row_1 = next(
                item
                for item in self._extract_rows(resp_1.json())
                if item.get("group") == "PUNE" or item.get("cust_location") == "PUNE"
            )
            self.assertEqual(
                Decimal(str(pune_row_1["total_transaction_volume"])),
                Decimal("1000.00"),
            )

            # 2nd call: warm cache -> HIT
            resp_2 = self.client.get(url)
            self.assertEqual(resp_2.status_code, status.HTTP_200_OK)
            self.assertEqual(resp_2["X-Cache"], "HIT")

            # Ingest second batch (adds 2500.00 to PUNE -> triggers cache invalidation)
            batch_2 = _write_csv(
                self.processing_dir / "analytics_batch_2.csv",
                [
                    [
                        "TXN_CACHE_002",
                        "CUST2002",
                        "18/09/90",
                        "M",
                        "PUNE",
                        "200000.00",
                        "23/11/26",
                        "111500",
                        "2500.00",
                    ],
                ],
            )
            job_2 = IngestionJob.objects.create(
                file_name="analytics_batch_2.csv",
                source="AUTO_DROP",
                status="PENDING",
            )
            process_csv_file(str(batch_2), str(job_2.job_id))

            # 3rd call: invalidated cache -> MISS with updated 3500.00 total
            resp_3 = self.client.get(url)
            self.assertEqual(resp_3.status_code, status.HTTP_200_OK)
            self.assertEqual(resp_3["X-Cache"], "MISS")
            pune_row_3 = next(
                item
                for item in self._extract_rows(resp_3.json())
                if item.get("group") == "PUNE" or item.get("cust_location") == "PUNE"
            )
            self.assertEqual(
                Decimal(str(pune_row_3["total_transaction_volume"])),
                Decimal("3500.00"),
            )

            # 4th call: re-warmed cache -> HIT
            resp_4 = self.client.get(url)
            self.assertEqual(resp_4.status_code, status.HTTP_200_OK)
            self.assertEqual(resp_4["X-Cache"], "HIT")


class JWTAuthenticationAndDocsTests(APITransactionTestCase):
    """Verify JWT login, refresh rotation/blacklisting, 401 enforcement, and public docs."""

    def setUp(self):
        self.username = "jwt_auditor"
        self.password = "StrongBankPass123!"
        user_model = get_user_model()
        self.user = user_model.objects.create_user(
            username=self.username,
            password=self.password,
        )
        self.token_url = reverse("token_obtain_pair")
        self.refresh_url = reverse("token_refresh")
        self.analytics_url = "/api/transactions/analytics/?group_by=location"
        self.docs_url = reverse("swagger-ui")
        self.schema_url = reverse("schema")

    def test_unauthenticated_request_to_protected_endpoint_returns_401(self):
        """Protected API endpoints must reject requests without a valid Bearer token."""
        response = self.client.get(self.analytics_url)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_jwt_obtain_and_authenticated_access(self):
        """Valid credentials return access + refresh tokens that unlock protected routes."""
        token_resp = self.client.post(
            self.token_url,
            {"username": self.username, "password": self.password},
            format="json",
        )
        self.assertEqual(token_resp.status_code, status.HTTP_200_OK)
        self.assertIn("access", token_resp.data)
        self.assertIn("refresh", token_resp.data)

        access_token = token_resp.data["access"]
        self.client.credentials(HTTP_AUTHORIZATION=f"Bearer {access_token}")

        analytics_resp = self.client.get(self.analytics_url)
        self.assertEqual(analytics_resp.status_code, status.HTTP_200_OK)

    def test_refresh_token_rotation_and_blacklist_replay_rejection(self):
        """
        Refreshing issues a new access + refresh pair and blacklists the old refresh token.
        Replaying the old refresh token must fail with 401 Unauthorized.
        """
        token_resp = self.client.post(
            self.token_url,
            {"username": self.username, "password": self.password},
            format="json",
        )
        self.assertEqual(token_resp.status_code, status.HTTP_200_OK)
        old_refresh = token_resp.data["refresh"]

        # 1st refresh: succeeds and rotates the refresh token
        refresh_resp = self.client.post(
            self.refresh_url,
            {"refresh": old_refresh},
            format="json",
        )
        self.assertEqual(refresh_resp.status_code, status.HTTP_200_OK)
        self.assertIn("access", refresh_resp.data)
        self.assertIn("refresh", refresh_resp.data)
        self.assertNotEqual(refresh_resp.data["refresh"], old_refresh)

        # New access token works immediately
        self.client.credentials(
            HTTP_AUTHORIZATION=f"Bearer {refresh_resp.data['access']}"
        )
        analytics_resp = self.client.get(self.analytics_url)
        self.assertEqual(analytics_resp.status_code, status.HTTP_200_OK)

        # Replaying the rotated (blacklisted) refresh token must be rejected
        replay_resp = self.client.post(
            self.refresh_url,
            {"refresh": old_refresh},
            format="json",
        )
        self.assertEqual(replay_resp.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_swagger_docs_and_schema_are_publicly_accessible(self):
        """Swagger UI (/api/docs/) and OpenAPI schema (/api/schema/) return 200 without auth."""
        docs_resp = self.client.get(self.docs_url)
        self.assertEqual(docs_resp.status_code, status.HTTP_200_OK)

        schema_resp = self.client.get(self.schema_url)
        self.assertEqual(schema_resp.status_code, status.HTTP_200_OK)