# Financial Data Pipeline (`fin_data_pipeline`)

[![Enterprise CI Pipeline](https://github.com/priyankaathakare/fin_data_pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/priyankaathakare/fin_data_pipeline/actions/workflows/ci.yml)
![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![Django & DRF](https://img.shields.io/badge/Django%20%7C%20DRF-REST%20API-092E20?logo=django&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-COPY%20Engine-336791?logo=postgresql&logoColor=white)
![Celery & Redis](https://img.shields.io/badge/Celery%20%7C%20Redis-Async%20ETL-DC382D?logo=redis&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker-6--Service%20Stack-2496ED?logo=docker&logoColor=white)

An enterprise-grade asynchronous financial CSV ingestion and analytics backend built with **Django**, **Django REST Framework (DRF)**, **Celery**, **Redis**, **PostgreSQL**, and **Docker Compose**. Designed for high-throughput bulk loading, fault-tolerant dead-letter quarantine, real-time task observability, and low-latency cached reporting.

---

## Key Features

* **High-Throughput PostgreSQL `COPY` Engine:** Streams large CSV datasets in 50,000-row chunks into session-scoped PostgreSQL `TEMP` tables using native `COPY`, then merges records idempotently via `DISTINCT ON` + `INSERT ... ON CONFLICT DO NOTHING` inside atomic transactions.
* **Dual Ingestion Modes:** Supports automated drop-zone polling every 30 seconds via **Celery Beat** (`AUTO_DROP`) and on-demand JWT-authenticated CSV uploads via DRF (`API_UPLOAD`) returning `202 Accepted`.
* **Fault Tolerance & Dead-Letter Quarantine:** Retries transient database errors (`OperationalError`) using exponential backoff while failing fast on malformed/corrupt files (`UnicodeDecodeError`, invalid schemas) and isolating them in `data_drop/failed/` to prevent infinite retry loops.
* **Real-Time Observability & One-Click Recovery:** Monitors live Celery worker states (`RECEIVED` -> `STARTED` -> `SUCCESS` / `FAILURE`) in **Celery Flower** and audits every batch in a custom color-coded **Django Admin** panel featuring a one-click **`Retry selected FAILED ingestion jobs`** recovery action.
* **Redis-Cached Analytics API:** Serves aggregated transaction metrics (`GET /api/transactions/analytics/`) backed by an isolated Redis cache (`DB 1`) with `X-Cache: HIT/MISS` telemetry headers and `O(1)` version-key invalidation triggered automatically upon batch completion.
* **Automated CI Pipeline:** Enforces **Ruff** linting, Django system and migration checks, **OpenAPI 3.0** (`drf-spectacular`) schema validation, automated tests against live PostgreSQL/Redis containers, and Docker image builds on every push via **GitHub Actions**.

---

## System Architecture

```text
  [data_drop/*.csv]                 [POST /api/transactions/upload/ (JWT)]
         |                                            |
         v (Celery Beat: every 30s)                   v (202 Accepted)
  scan_drop_zone() --> Move to data_drop/processing/  |
         |                                            |
         +---------------------+----------------------+
                               |
                               v
                 [Redis DB 0: Celery Broker] <-----> [Celery Flower :5555]
                               |                      (Live Events: -E)
                               v
                 [Celery Worker: process_csv_file]
                    |                        |
        (Success)   v                        v (Fatal Error / Retries Exhausted)
  +----------------------------------+   +-------------------------------------+
  | PostgreSQL Atomic Transaction:   |   | Move CSV -> data_drop/failed/       |
  | 1. COPY 50k chunks -> TEMP TABLE |   | Mark IngestionJob = FAILED + error  |
  | 2. INSERT DISTINCT ON CONFLICT   |   | Recoverable via Django Admin Retry  |
  | 3. Mark IngestionJob = COMPLETED |   +-------------------------------------+
  | 4. Move CSV -> data_drop/processed/
  +----------------------------------+
                    |
                    v (Event-Driven Cache Invalidation)
  [Redis DB 1: INCR analytics_version] ---> [GET /api/transactions/analytics/]
                                             (X-Cache: MISS -> HIT)
```

---

## Tech Stack

| Layer | Technology | Role |
| :--- | :--- | :--- |
| **Web & API** | Python 3.12, Django, DRF, SimpleJWT, `drf-spectacular` | Authenticated REST APIs, Swagger/OpenAPI 3.0 docs, Admin dashboard |
| **Async ETL & Scheduling** | Celery, Celery Beat | Background CSV ingestion workers, exponential backoff retries, 30s drop-zone scanner |
| **Database** | PostgreSQL | Atomic `COPY` bulk ingestion, idempotent upserts, `IngestionJob` audit ledger |
| **Broker & Cache** | Redis 7 (DB 0 & DB 1) | DB 0 for Celery broker/results; DB 1 for isolated versioned analytics caching |
| **Monitoring** | Celery Flower 2.0 | Live worker health, task state timelines, runtime metrics, and stack traces |
| **Infrastructure & CI** | Docker Compose, GitHub Actions, Ruff | 6-service local orchestration, automated lint/migration/schema/Docker CI gates |

---

## Project Structure

```text
fin_data_pipeline/
├── .github/workflows/
│   └── ci.yml                 # Ruff lint, Postgres/Redis Django checks, OpenAPI validation, Docker build
├── core/
│   ├── celery.py              # Celery app & Beat schedule configuration
│   ├── settings.py            # PostgreSQL, Redis DB 0/DB 1, DRF, JWT, and Spectacular settings
│   └── urls.py                # Admin, JWT auth, Swagger UI, and transactions API routing
├── transactions/
│   ├── admin.py               # Color-coded IngestionJob badges & one-click dead-letter retry action
│   ├── cache.py               # O(1) versioned Redis analytics cache & invalidation helpers
│   ├── models.py              # TransactionRecord & IngestionJob audit models
│   ├── serializers.py         # Transaction, IngestionJob, and TransactionAnalytics serializers
│   ├── tasks.py               # scan_drop_zone & chunked PostgreSQL COPY process_csv_file tasks
│   ├── urls.py                # Upload, jobs audit, and analytics endpoints
│   └── views.py               # DRF views with JWT auth and X-Cache telemetry headers
├── data_drop/                 # Runtime drop zone (gitignored): processing/, processed/, failed/
├── docker-compose.yml         # 6-service stack: db, redis, web, celery, celery-beat, flower
├── Dockerfile                 # Python 3.12 application container
└── requirements.txt
```

---

## Quick Start (Docker Compose)

### 1. Clone the Repository
```bash
git clone [https://github.com/priyankaathakare/fin_data_pipeline.git](https://github.com/priyankaathakare/fin_data_pipeline.git)
cd fin_data_pipeline
```

### 2. Configure Environment Variables
Create a `.env` file in the project root:

```env
SECRET_KEY=change-me-in-production
DEBUG=True
ALLOWED_HOSTS=localhost,127.0.0.1

POSTGRES_DB=fin_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=fin_password
POSTGRES_HOST=db
POSTGRES_PORT=5432

CELERY_BROKER_URL=redis://redis:6379/0
CELERY_RESULT_BACKEND=redis://redis:6379/0
REDIS_CACHE_URL=redis://redis:6379/1
```

### 3. Build & Start All 6 Services
```bash
docker compose up -d --build
```

### 4. Apply Migrations & Create an Admin User
```bash
docker compose exec web python manage.py migrate
docker compose exec web python manage.py createsuperuser
```

---

## Dashboards & API Endpoints

### Operational Dashboards
| Service | URL | Purpose |
| :--- | :--- | :--- |
| **Swagger UI (OpenAPI 3.0)** | `http://localhost:8000/api/docs/` | Interactive API testing with JWT Bearer auth |
| **Django Admin Audit Panel** | `http://localhost:8000/admin/transactions/ingestionjob/` | Color-coded job statuses, row counts, durations, error logs, and one-click retry |
| **Celery Flower** | `http://127.0.0.1:5555` | Real-time worker status and live task execution telemetry |

### Core REST Endpoints
| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/api/token/` | Obtain JWT `access` and `refresh` tokens |
| `POST` | `/api/transactions/upload/` | Upload a CSV file for async ingestion (`202 Accepted` with `job_id`) |
| `GET` | `/api/transactions/jobs/` | Query `IngestionJob` execution history, row counts, timings, and errors |
| `GET` | `/api/transactions/analytics/?group_by=location` | Aggregated volume and average balance metrics (Redis-cached with `X-Cache` header) |

---

## CSV Processing Lifecycle & Dead-Letter Recovery

Every CSV dropped into `data_drop/` moves deterministically through isolated directories so Celery Beat never double-queues active or failing files:

1. **Intake (`data_drop/`):** Celery Beat runs `scan_drop_zone` every 30 seconds, creates an `IngestionJob` (`PENDING`), and atomically moves each `.csv` into `data_drop/processing/`.
2. **Bulk Ingestion (`data_drop/processing/`):** A Celery worker sets status to `PROCESSING` and streams 50,000-row chunks into PostgreSQL via `COPY` + `ON CONFLICT DO NOTHING`.
3. **Completion (`data_drop/processed/`):** On commit, the job switches to `COMPLETED`, the CSV archives to `data_drop/processed/`, and `invalidate_analytics_cache()` increments the Redis DB 1 cache version.
4. **Dead-Letter Quarantine (`data_drop/failed/`):** On unrecoverable encoding/schema errors (or after 3 transient DB retries), the job switches to `FAILED` with the diagnostic message persisted, and the file moves to `data_drop/failed/`.
5. **One-Click Admin Recovery:** After repairing a quarantined CSV in `data_drop/failed/`, select the red **`FAILED`** row in Django Admin (`http://localhost:8000/admin/transactions/ingestionjob/`), choose **`Retry selected FAILED ingestion jobs`**, and click **Go** to restage and re-queue the batch directly into Celery.

---

## Redis Caching & Verification

Analytics responses are cached in **Redis DB 1** (isolated from Celery's broker on DB 0) using versioned keys (`analytics:v<version>:<params>`). When any ingestion job completes, the version counter increments atomically in `O(1)`—avoiding blocking `KEYS *` scans.

To verify cache behavior via Swagger UI or `curl` after an ingestion job completes:
* **1st request** to `GET /api/transactions/analytics/?group_by=location` returns response header **`X-Cache: MISS`** (computed in PostgreSQL and stored in Redis).
* **2nd request** returns **`X-Cache: HIT`** directly from Redis in milliseconds.

---

## CI/CD & Local Verification

GitHub Actions (`.github/workflows/ci.yml`) automatically runs three sequential gates on pushes and pull requests to `main`:

1. **Code Quality & Linting:** `ruff check .`
2. **Django Checks, Schema & Tests (PostgreSQL + Redis):**
   * `python manage.py check`
   * `python manage.py makemigrations --check --dry-run`
   * `python manage.py migrate --noinput`
   * `python manage.py spectacular --file schema.yml --validate`
   * `python manage.py test`
3. **Docker Image Build Check:** `docker build -t fin_data_pipeline:ci .`

Run the same checks locally before pushing:

```bash
ruff check .
docker compose exec web python manage.py check
docker compose exec web python manage.py makemigrations --check --dry-run
docker compose exec web python manage.py spectacular --file schema.yml --validate
```