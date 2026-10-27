from __future__ import annotations

import os
from datetime import datetime, timezone

from etls.analytics_etl import build_summary, write_summary
from etls.data_quality import validate_posts
from etls.reddit_etl import (
    connect_reddit,
    enrich_features,
    extract_posts,
    load_data_to_csv,
    load_data_to_parquet,
    transform_data,
)
from utils.constants import OUTPUT_PATH, WRITE_PARQUET
from utils.logger import get_logger

log = get_logger(__name__)


def _coerce_limit(limit):
    if limit in (None, '', 'None', 'none', 'null'):
        return None
    return int(limit)


def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit=None, **context):

    limit = _coerce_limit(limit)
    started = datetime.now(timezone.utc)
    log.info('Starting reddit pipeline for r/%s (file=%s)', subreddit, file_name)

    reddit = connect_reddit()
    posts = extract_posts(reddit, subreddit, time_filter, limit)

    transformed = transform_data(posts)
    enriched = enrich_features(transformed)
    final_data, quality = validate_posts(enriched)

    base = f'{OUTPUT_PATH}/{file_name}'
    file_path = load_data_to_csv(final_data, f'{base}.csv')
    parquet_path = load_data_to_parquet(final_data, base) if WRITE_PARQUET else None

    run_date = started.strftime('%Y-%m-%d')
    summary = build_summary(final_data, subreddit=subreddit, run_date=run_date)
    summary['quality'] = quality.to_dict()
    summary['duration_seconds'] = round((datetime.now(timezone.utc) - started).total_seconds(), 2)
    summary_path = write_summary(summary, f'{base}_summary.json')

    ti = context.get('ti')
    if ti is not None:
        ds = context.get('ds') or run_date
        ti.xcom_push(key='parquet_path', value=parquet_path)
        ti.xcom_push(key='summary_path', value=summary_path)
        ti.xcom_push(key='subreddit', value=subreddit)
        ti.xcom_push(key='run_date', value=ds)
        ti.xcom_push(key='row_count', value=int(len(final_data)))
        ti.xcom_push(key='quality_report', value=quality.to_dict())

    log.info('Reddit pipeline finished: %d rows -> %s', len(final_data), os.path.basename(file_path))
    return file_path
