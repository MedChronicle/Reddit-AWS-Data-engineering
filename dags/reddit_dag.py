import os
import re
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.aws_s3_pipeline import upload_s3_pipeline
from pipelines.housekeeping_pipeline import cleanup_pipeline
from pipelines.postgres_pipeline import load_postgres_pipeline
from pipelines.reddit_pipeline import reddit_pipeline
from utils.alerts import notify_failure, notify_success
from utils.constants import LOAD_TO_POSTGRES, RETENTION_DAYS, SUBREDDITS

default_args = {
    'owner': 'Rishit Saharan',
    'start_date': datetime(2023, 10, 22),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(minutes=30),
    'on_failure_callback': notify_failure,
}

dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,                       
    dagrun_timeout=timedelta(hours=2),
    tags=['reddit', 'etl', 'pipeline'],
    # Trigger with config to override at run time, e.g. {"limit": 50, "time_filter": "week"}
    params={'time_filter': 'day', 'limit': 100},
    doc_md="""
### Reddit -> S3 / Postgres ETL
1. **run_reddit_pipeline** - extract top posts, clean, add features, run data-quality gate,
   write CSV + Parquet + JSON summary
2. **s3_upload** - partitioned upload to `raw/`, `processed/`, `reports/`
3. **postgres_load** - idempotent upsert into `reddit.posts` + daily snapshot
4. **cleanup_local_files** - delete local files older than the retention window

Subreddits come from `ETL_SUBREDDITS` / `[etl_settings] subreddits` (comma separated).
""",
)

RUN_DATE = '{{ ds_nodash }}'


def _slug(name: str) -> str:
    return re.sub(r'[^a-zA-Z0-9_]+', '_', name).strip('_').lower()


def _task_id(base: str, subreddit: str) -> str:
    # A single subreddit keeps the original task ids; several get a suffix
    return base if len(SUBREDDITS) == 1 else f'{base}__{_slug(subreddit)}'


postgres_tasks = []
upload_tasks = []

for subreddit in SUBREDDITS:
    extract_id = _task_id('run_reddit_pipeline', subreddit)

    reddit_pipeline_task = PythonOperator(
        task_id=extract_id,
        python_callable=reddit_pipeline,
        op_kwargs={
            'file_name': f'reddit_{_slug(subreddit)}_{RUN_DATE}',
            'subreddit': subreddit,
            'time_filter': '{{ params.time_filter }}',
            'limit': '{{ params.limit }}'
        },
        dag=dag
    )

    upload_s3 = PythonOperator(
        task_id=_task_id('s3_upload', subreddit),
        python_callable=upload_s3_pipeline,
        op_kwargs={'extract_task_id': extract_id},
        dag=dag
    )
    reddit_pipeline_task >> upload_s3
    upload_tasks.append(upload_s3)

    if LOAD_TO_POSTGRES:
        load_postgres = PythonOperator(
            task_id=_task_id('postgres_load', subreddit),
            python_callable=load_postgres_pipeline,
            op_kwargs={'extract_task_id': extract_id},
            dag=dag
        )
        reddit_pipeline_task >> load_postgres
        postgres_tasks.append(load_postgres)

cleanup = PythonOperator(
    task_id='cleanup_local_files',
    python_callable=cleanup_pipeline,
    op_kwargs={'retention_days': RETENTION_DAYS},
    trigger_rule='all_success',
    on_success_callback=notify_success,
    dag=dag
)
upload_tasks + postgres_tasks >> cleanup
