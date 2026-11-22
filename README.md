# Financial Data Pipeline (`fin_data_pipeline`)

[![Enterprise CI Pipeline](https://github.com/priyankaathakare/fin_data_pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/priyankaathakare/fin_data_pipeline/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![Django](https://img.shields.io/badge/Django-DRF-092E20?logo=django&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15%2B-336791?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Ruff](https://img.shields.io/badge/Linting-Ruff-D7FF64?logo=ruff&logoColor=black)

An enterprise-grade, containerized financial transaction ingestion and reporting pipeline built with **Django, Django REST Framework (DRF), Celery, Redis, and PostgreSQL**.

Designed for high-throughput CSV ingestion, the pipeline combines an automated **Celery Beat drop-zone scanner** with **PostgreSQL `COPY` staging-to-target bulk loading**, **idempotent upserts**, **full `IngestionJob` audit logging**, **JWT authentication**, **OpenAPI 3.0 / Swagger documentation**, and an automated **GitHub Actions CI pipeline**.

---

## Architecture Overview

```text
                  ┌───────────────────────────┐
                  │   CSV File Drop Zone /    │
                  │   REST API File Upload    │
                  └─────────────┬─────────────┘
                                │
             ┌──────────────────┴──────────────────┐
             ▼                                     ▼
  ┌────────────────────┐                ┌────────────────────┐
  │    Celery Beat     │                │   Django + DRF     │
  │ (Drop-Zone Scanner)│                │  (JWT Auth + Docs) │
  └─────────┬──────────┘                └─────────┬──────────┘
            │ Enqueues Task                       │ Enqueues Task /
            ▼                                     │ Serves Queries
  ┌────────────────────┐                          │
  │    Redis Broker    │◄─────────────────────────┘
  └─────────┬──────────┘
            │
            ▼
  ┌──────────────────────────────────────────────────────────┐
  │                     Celery Worker                        │
  │  1. Create IngestionJob audit record (PROCESSING)        │
  │  2. Bulk-load CSV into TEMP staging table via PG COPY    │
  │  3. Idempotent INSERT ... ON CONFLICT into target table  │
  │  4. Update IngestionJob metrics (rows, duration, status) │
  └─────────────────────────────┬────────────────────────────┘
                                │
                                ▼
  ┌──────────────────────────────────────────────────────────┐
  │                      PostgreSQL                          │
  │  • transaction_records (B-tree indexed for fast filters) │
  │  • transactions_ingestionjob (immutable audit history)   │
  └──────────────────────────────────────────────────────────┘