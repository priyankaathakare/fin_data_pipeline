import os

from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "core.settings")

app = Celery("core")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

SCAN_INTERVAL_SECONDS = float(
    os.environ.get("DROP_ZONE_SCAN_INTERVAL_SECONDS", "30")
)

app.conf.beat_schedule = {
    "scan-csv-drop-zone": {
        "task": "transactions.tasks.scan_drop_zone",
        "schedule": SCAN_INTERVAL_SECONDS,
    },
}