from __future__ import annotations

import pandas as pd

from etls.postgres_etl import load_posts_to_postgres
from utils.logger import get_logger

log = get_logger(__name__)

DEFAULT_EXTRACT_TASK_ID = 'run_reddit_pipeline'


def load_postgres_pipeline(ti, extract_task_id: str = DEFAULT_EXTRACT_TASK_ID):
    parquet_path = ti.xcom_pull(task_ids=extract_task_id, key='parquet_path')
    csv_path = ti.xcom_pull(task_ids=extract_task_id, key='return_value')
    run_date = ti.xcom_pull(task_ids=extract_task_id, key='run_date')

    if parquet_path:
        df = pd.read_parquet(parquet_path)
    elif csv_path:
        df = pd.read_csv(csv_path, parse_dates=['created_utc', 'edited_at', 'extracted_at'])
    else:
        raise ValueError(f'No data path in XCom from task "{extract_task_id}"')

    return load_posts_to_postgres(df, snapshot_date=run_date)
