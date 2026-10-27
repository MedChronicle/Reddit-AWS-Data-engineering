from __future__ import annotations

from etls.housekeeping import cleanup_old_files
from utils.constants import OUTPUT_PATH, RETENTION_DAYS


def cleanup_pipeline(retention_days: int | None = None):
    days = RETENTION_DAYS if retention_days is None else int(retention_days)
    return cleanup_old_files(OUTPUT_PATH, days)
