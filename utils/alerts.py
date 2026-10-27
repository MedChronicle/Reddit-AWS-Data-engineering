"""Failure / success notifications for Airflow callbacks.

Always logs. Additionally posts to Slack when ``SLACK_WEBHOOK_URL`` is configured.
Alerting must never break the pipeline, so every error in here is swallowed.
"""
from __future__ import annotations

import requests

from utils.constants import SLACK_WEBHOOK_URL
from utils.logger import get_logger

log = get_logger(__name__)


def _post_to_slack(text: str) -> bool:
    if not SLACK_WEBHOOK_URL:
        return False
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json={'text': text}, timeout=10)
        response.raise_for_status()
        return True
    except Exception as exc:  # noqa: BLE001 - alerting must not raise
        log.warning('Could not send Slack alert: %s', exc)
        return False


def notify_failure(context: dict) -> None:
    """Airflow ``on_failure_callback``."""
    ti = context.get('task_instance')
    dag_id = getattr(ti, 'dag_id', 'unknown_dag')
    task_id = getattr(ti, 'task_id', 'unknown_task')
    run_id = context.get('run_id', 'unknown_run')
    error = context.get('exception')
    message = f':red_circle: *{dag_id}.{task_id}* failed (run `{run_id}`)\n```{error}```'
    log.error('Task failed: %s.%s run=%s error=%s', dag_id, task_id, run_id, error)
    _post_to_slack(message)


def notify_success(context: dict) -> None:
    """Airflow ``on_success_callback`` (use on the last task of a DAG)."""
    dag_id = context.get('dag').dag_id if context.get('dag') else 'unknown_dag'
    run_id = context.get('run_id', 'unknown_run')
    log.info('DAG run succeeded: %s run=%s', dag_id, run_id)
    _post_to_slack(f':white_check_mark: *{dag_id}* run `{run_id}` completed')
