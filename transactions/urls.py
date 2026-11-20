from django.urls import path
from .views import (
    TransactionUploadView,
    TransactionListView,
    IngestionJobListView,
    IngestionJobDetailView,
)

urlpatterns = [
    path("upload/", TransactionUploadView.as_view(), name="transaction-upload"),
    path("list/", TransactionListView.as_view(), name="transaction-list"),
    path("jobs/", IngestionJobListView.as_view(), name="ingestion-job-list"),
    path(
        "jobs/<uuid:job_id>/",
        IngestionJobDetailView.as_view(),
        name="ingestion-job-detail",
    ),
]