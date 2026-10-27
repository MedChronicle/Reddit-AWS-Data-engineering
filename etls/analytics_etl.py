from __future__ import annotations

import json
import os

import pandas as pd

from utils.logger import get_logger

log = get_logger(__name__)


def build_summary(df: pd.DataFrame, subreddit: str = '', run_date: str = '', top_n: int = 5) -> dict:
    if df.empty:
        return {'subreddit': subreddit, 'run_date': run_date, 'rows': 0}

    top_posts = df.nlargest(top_n, 'score')[['id', 'title', 'score', 'num_comments', 'author']]
    hot_posts = df.nlargest(top_n, 'hotness')[['id', 'title', 'hotness']] if 'hotness' in df else top_posts[['id', 'title']]
    top_authors = df.groupby('author')['score'].sum().nlargest(top_n)

    summary = {
        'subreddit': subreddit,
        'run_date': run_date,
        'rows': int(len(df)),
        'total_score': int(df['score'].sum()),
        'avg_score': round(float(df['score'].mean()), 2),
        'median_score': float(df['score'].median()),
        'total_comments': int(df['num_comments'].sum()),
        'avg_comments': round(float(df['num_comments'].mean()), 2),
        'pct_questions': round(float(df['is_question'].mean() * 100), 1) if 'is_question' in df else None,
        'pct_edited': round(float(df['edited'].mean() * 100), 1),
        'pct_nsfw': round(float(df['over_18'].mean() * 100), 1),
        'top_posts': top_posts.to_dict(orient='records'),
        'hottest_posts': hot_posts.to_dict(orient='records'),
        'top_authors_by_score': {str(k): int(v) for k, v in top_authors.items()},
    }
    if 'primary_tag' in df:
        summary['posts_by_tag'] = {str(k): int(v) for k, v in df['primary_tag'].value_counts().items()}
    if 'post_hour_utc' in df:
        summary['posts_by_hour_utc'] = {str(int(k)): int(v) for k, v in df['post_hour_utc'].value_counts().sort_index().items()}
    if 'url_domain' in df:
        summary['top_domains'] = {str(k): int(v) for k, v in df['url_domain'].value_counts().head(5).items()}
    return summary


def write_summary(summary: dict, file_path: str) -> str:
    directory = os.path.dirname(file_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as handle:
        json.dump(summary, handle, indent=2, default=str)
    log.info('Wrote run summary to %s', file_path)
    return file_path
