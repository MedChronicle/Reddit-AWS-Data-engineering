from __future__ import annotations

import os

from etls.aws_etl import build_s3_key, connect_to_s3, create_bucket_if_not_exist, upload_to_s3
from utils.constants import AWS_BUCKET_NAME, S3_PROCESSED_PREFIX, S3_RAW_PREFIX, S3_REPORTS_PREFIX
from utils.logger import get_logger

log = get_logger(__name__)

DEFAULT_EXTRACT_TASK_ID = 'run_reddit_pipeline'


def upload_s3_pipeline(ti, extract_task_id: str = DEFAULT_EXTRACT_TASK_ID):
    file_path = ti.xcom_pull(task_ids=extract_task_id, key='return_value')
    if not file_path:
        raise ValueError(f'No file path in XCom from task "{extract_task_id}" - did the extract task succeed?')
    parquet_path = ti.xcom_pull(task_ids=extract_task_id, key='parquet_path')
    summary_path = ti.xcom_pull(task_ids=extract_task_id, key='summary_path')
    subreddit = ti.xcom_pull(task_ids=extract_task_id, key='subreddit')
    run_date = ti.xcom_pull(task_ids=extract_task_id, key='run_date')

    s3 = connect_to_s3()
    create_bucket_if_not_exist(s3, AWS_BUCKET_NAME)

    uploaded = {}
    for path, prefix, label in (
        (file_path, S3_RAW_PREFIX, 'csv'),
        (parquet_path, S3_PROCESSED_PREFIX, 'parquet'),
        (summary_path, S3_REPORTS_PREFIX, 'summary'),
    ):
        if not path:
            continue
        name = os.path.basename(path)
        relative = build_s3_key('', name, subreddit, run_date)
        uploaded[label] = upload_to_s3(s3, path, AWS_BUCKET_NAME, relative, prefix=prefix)
    return uploaded
