from __future__ import annotations

import os
import time

from utils.logger import get_logger

log = get_logger(__name__)

_EXTENSIONS = ('.csv', '.parquet', '.json')


def cleanup_old_files(directory: str, retention_days: int, extensions: tuple = _EXTENSIONS) -> list[str]:
    removed: list[str] = []
    if retention_days <= 0 or not os.path.isdir(directory):
        return removed
    cutoff = time.time() - retention_days * 86400
    for name in os.listdir(directory):
        path = os.path.join(directory, name)
        if os.path.isfile(path) and name.endswith(extensions) and os.path.getmtime(path) < cutoff:
            os.remove(path)
            removed.append(path)
    if removed:
        log.info('Removed %d files older than %d days from %s', len(removed), retention_days, directory)
    return removed
