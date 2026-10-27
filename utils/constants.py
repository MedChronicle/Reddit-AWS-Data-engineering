
from __future__ import annotations

import configparser
import os
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = Path(os.environ.get('PIPELINE_CONFIG', PROJECT_ROOT / 'config' / 'config.conf'))

parser = configparser.ConfigParser(interpolation=None)
parser.read(CONFIG_PATH)

_PLACEHOLDER = re.compile(r'^\s*\[.*\]\s*$')


def get_setting(section: str, key: str, env: str | None = None, default=''):
    value = os.environ.get(env) if env else None
    if value is None or value == '':
        value = parser.get(section, key, fallback=None)
    if value is None or value.strip() == '' or _PLACEHOLDER.match(value):
        return default
    return value.strip()


def _as_int(value, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_bool(value, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value in (None, ''):
        return default
    return str(value).strip().lower() in {'1', 'true', 'yes', 'y', 'on'}


def _as_list(value) -> list[str]:
    return [item.strip() for item in str(value).split(',') if item.strip()]

SECRET = get_setting('api_keys', 'reddit_secret_key', 'REDDIT_SECRET_KEY')
CLIENT_ID = get_setting('api_keys', 'reddit_client_id', 'REDDIT_CLIENT_ID')
# Reddit's API rules ask for a descriptive, unique user agent
USER_AGENT = get_setting('api_keys', 'reddit_user_agent', 'REDDIT_USER_AGENT',
                         'script:reddit-aws-data-engineering:v2.0 (by /u/airscholar-agent)')

DATABASE_HOST = get_setting('database', 'database_host', 'DATABASE_HOST', 'localhost')
DATABASE_NAME = get_setting('database', 'database_name', 'DATABASE_NAME', 'airflow_reddit')
DATABASE_PORT = get_setting('database', 'database_port', 'DATABASE_PORT', '5432')
DATABASE_USER = get_setting('database', 'database_username', 'DATABASE_USER', 'postgres')
DATABASE_PASSWORD = get_setting('database', 'database_password', 'DATABASE_PASSWORD', 'postgres')
DATABASE_SCHEMA = get_setting('database', 'database_schema', 'DATABASE_SCHEMA', 'reddit')
LOAD_TO_POSTGRES = _as_bool(get_setting('database', 'load_to_postgres', 'LOAD_TO_POSTGRES', 'true'), True)

AWS_ACCESS_KEY_ID = get_setting('aws', 'aws_access_key_id', 'AWS_ACCESS_KEY_ID')
AWS_ACCESS_KEY = get_setting('aws', 'aws_secret_access_key', 'AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = get_setting('aws', 'aws_session_token', 'AWS_SESSION_TOKEN')
AWS_REGION = get_setting('aws', 'aws_region', 'AWS_REGION')
AWS_BUCKET_NAME = get_setting('aws', 'aws_bucket_name', 'AWS_BUCKET_NAME')
AWS_ENDPOINT_URL = get_setting('aws', 'aws_endpoint_url', 'AWS_ENDPOINT_URL')

AWS_HARDEN_BUCKET = _as_bool(get_setting('aws', 'harden_bucket', 'AWS_HARDEN_BUCKET', 'true'), True)
S3_RAW_PREFIX = get_setting('aws', 's3_raw_prefix', 'S3_RAW_PREFIX', 'raw')
S3_PROCESSED_PREFIX = get_setting('aws', 's3_processed_prefix', 'S3_PROCESSED_PREFIX', 'processed')
S3_REPORTS_PREFIX = get_setting('aws', 's3_reports_prefix', 'S3_REPORTS_PREFIX', 'reports')

INPUT_PATH = get_setting('file_paths', 'input_path', 'INPUT_PATH', str(PROJECT_ROOT / 'data' / 'input'))
OUTPUT_PATH = get_setting('file_paths', 'output_path', 'OUTPUT_PATH', str(PROJECT_ROOT / 'data' / 'output'))

BATCH_SIZE = _as_int(get_setting('etl_settings', 'batch_size', 'ETL_BATCH_SIZE', '100'), 100)

ERROR_HANDLING = get_setting('etl_settings', 'error_handling', 'ETL_ERROR_HANDLING', 'abort').lower()
LOG_LEVEL = get_setting('etl_settings', 'log_level', 'ETL_LOG_LEVEL', 'info').upper()
SUBREDDITS = _as_list(get_setting('etl_settings', 'subreddits', 'ETL_SUBREDDITS', 'dataengineering'))
RETENTION_DAYS = _as_int(get_setting('etl_settings', 'retention_days', 'ETL_RETENTION_DAYS', '14'), 14)
MIN_ROWS = _as_int(get_setting('etl_settings', 'min_rows', 'ETL_MIN_ROWS', '1'), 1)
WRITE_PARQUET = _as_bool(get_setting('etl_settings', 'write_parquet', 'ETL_WRITE_PARQUET', 'true'), True)
SELFTEXT_MAX_CHARS = _as_int(get_setting('etl_settings', 'selftext_max_chars', 'ETL_SELFTEXT_MAX_CHARS', '2000'), 2000)

SLACK_WEBHOOK_URL = get_setting('alerts', 'slack_webhook_url', 'SLACK_WEBHOOK_URL')

POST_FIELDS = (
    'id',
    'title',
    'score',
    'num_comments',
    'author',
    'created_utc',
    'url',
    'over_18',
    'edited',
    'spoiler',
    'stickied'
)

EXTRA_POST_FIELDS = (
    'subreddit',
    'upvote_ratio',
    'link_flair_text',
    'domain',
    'is_self',
    'permalink',
    'total_awards_received',
    'num_crossposts',
    'selftext',
)

ALL_POST_FIELDS = POST_FIELDS + EXTRA_POST_FIELDS

REQUIRED_COLUMNS = ('id', 'title', 'score', 'num_comments', 'author', 'created_utc', 'url')

TOPIC_KEYWORDS = {
    'spark': r'\b(?:spark|pyspark|databricks)\b',
    'warehouse': r'\b(?:snowflake|redshift|bigquery|synapse|warehouse|dwh)\b',
    'lakehouse': r'\b(?:iceberg|delta lake|delta|hudi|lakehouse|parquet)\b',
    'orchestration': r'\b(?:airflow|dagster|prefect|orchestrat\w*|dbt)\b',
    'streaming': r'\b(?:kafka|flink|kinesis|streaming|real[- ]?time|cdc)\b',
    'cloud': r'\b(?:aws|azure|gcp|s3|glue|lambda|fabric|cloud)\b',
    'sql': r'\b(?:sql|postgres\w*|mysql|query|queries|cte)\b',
    'python': r'\b(?:python|pandas|polars|duckdb)\b',
    'career': r'\b(?:career|salary|interview|job|offer|resume|cv|hiring|layoffs?|senior|junior|promotion)\b',
    'ai_ml': r'\b(?:ai|llm|genai|ml|machine learning|rag|vector)\b',
    'quality_governance': r'\b(?:data quality|governance|lineage|catalog|testing|observability)\b',
}
