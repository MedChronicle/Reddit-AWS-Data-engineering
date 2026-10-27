from __future__ import annotations

import math
import os
import re
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import praw
import prawcore
from praw import Reddit
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from utils.constants import (
    ALL_POST_FIELDS,
    CLIENT_ID,
    POST_FIELDS,
    SECRET,
    SELFTEXT_MAX_CHARS,
    TOPIC_KEYWORDS,
    USER_AGENT,
)
from utils.logger import get_logger

log = get_logger(__name__)

_RETRYABLE = (
    prawcore.exceptions.RequestException,
    prawcore.exceptions.ServerError,
    prawcore.exceptions.TooManyRequests,
)


class ConfigError(RuntimeError):


def connect_reddit() -> Reddit:
    if not CLIENT_ID or not SECRET:
        raise ConfigError(
            'Reddit credentials are missing. Set REDDIT_CLIENT_ID / REDDIT_SECRET_KEY '
            'or fill the [api_keys] section of config/config.conf.'
        )
    try:
        reddit = praw.Reddit(
            client_id=CLIENT_ID,
            client_secret=SECRET,
            user_agent=USER_AGENT,
            check_for_async=False,
        )
        reddit.read_only = True
        reddit.auth.scopes()
    except Exception as exc:
        log.error('Could not connect to Reddit: %s', exc)
        raise
    log.info('Connected to Reddit')
    return reddit


@retry(
    retry=retry_if_exception_type(_RETRYABLE),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    reraise=True,
)
def _fetch_posts(reddit: Reddit, subreddit: str, time_filter: str, limit) -> list:
    posts = []
    for post in reddit.subreddit(subreddit).top(time_filter=time_filter, limit=limit):
        post_dict = vars(post)
        posts.append({key: post_dict.get(key) for key in ALL_POST_FIELDS})
    return posts


def extract_posts(reddit: Reddit, subreddit: str, time_filter='day', limit=None):
    posts = _fetch_posts(reddit, subreddit, time_filter, limit)
    log.info('Extracted %d posts from r/%s (time_filter=%s, limit=%s)',
             len(posts), subreddit, time_filter, limit)
    return posts

def _author_name(value) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return '[deleted]'
    return str(value)


def _edited_flag(value) -> bool:
    if value is None or value is False:
        return False
    if isinstance(value, float) and math.isnan(value):
        return False
    return True


def _edited_at(value):
    if isinstance(value, (int, float)) and not isinstance(value, bool) and not math.isnan(value):
        return pd.to_datetime(value, unit='s')
    return pd.NaT


def transform_data(posts: list):
    post_df = pd.DataFrame(posts)

    for column in ALL_POST_FIELDS:
        if column not in post_df.columns:
            post_df[column] = None

    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = np.where((post_df['over_18'] == True), True, False)  # noqa: E712
    post_df['author'] = post_df['author'].map(_author_name)

    raw_edited = post_df['edited'].astype(object)
    post_df['edited_at'] = pd.to_datetime(raw_edited.map(_edited_at))
    post_df['edited'] = raw_edited.map(_edited_flag).astype(bool)

    post_df['num_comments'] = pd.to_numeric(post_df['num_comments'], errors='coerce').fillna(0).astype(int)
    post_df['score'] = pd.to_numeric(post_df['score'], errors='coerce').fillna(0).astype(int)
    post_df['title'] = post_df['title'].astype(str).str.strip()

    for flag in ('spoiler', 'stickied', 'is_self'):
        post_df[flag] = post_df[flag].map(lambda v: bool(v) if v is not None and v == v else False).astype(bool)

    post_df['subreddit'] = post_df['subreddit'].map(lambda v: '' if v is None else str(v))
    post_df['upvote_ratio'] = pd.to_numeric(post_df['upvote_ratio'], errors='coerce')
    post_df['total_awards_received'] = pd.to_numeric(post_df['total_awards_received'], errors='coerce').fillna(0).astype(int)
    post_df['num_crossposts'] = pd.to_numeric(post_df['num_crossposts'], errors='coerce').fillna(0).astype(int)
    post_df['link_flair_text'] = post_df['link_flair_text'].map(lambda v: v if isinstance(v, str) else None)
    post_df['selftext'] = post_df['selftext'].map(
        lambda v: v[:SELFTEXT_MAX_CHARS] if isinstance(v, str) else ''
    )
    post_df['permalink'] = post_df['permalink'].map(
        lambda v: f'https://www.reddit.com{v}' if isinstance(v, str) and v.startswith('/') else v
    )

    before = len(post_df)
    post_df = (
        post_df.sort_values('score', ascending=False, kind='stable')
        .drop_duplicates(subset='id', keep='first')
        .reset_index(drop=True)
    )
    if len(post_df) != before:
        log.warning('Dropped %d duplicate post ids', before - len(post_df))

    ordered = list(POST_FIELDS) + [c for c in post_df.columns if c not in POST_FIELDS]
    return post_df[ordered]


def _url_domain(url) -> str | None:
    if not isinstance(url, str) or not url:
        return None
    host = urlparse(url).netloc.lower()
    return host[4:] if host.startswith('www.') else (host or None)


def enrich_features(df: pd.DataFrame, reference_time=None) -> pd.DataFrame:
    out = df.copy()
    now = pd.Timestamp(reference_time) if reference_time is not None else pd.Timestamp.now(tz='UTC').tz_localize(None)

    out['extracted_at'] = now
    out['post_date'] = out['created_utc'].dt.strftime('%Y-%m-%d')
    out['post_hour_utc'] = out['created_utc'].dt.hour
    out['post_weekday'] = out['created_utc'].dt.day_name()

    out['title_length'] = out['title'].str.len()
    out['title_word_count'] = out['title'].str.split().str.len()
    out['is_question'] = out['title'].str.rstrip().str.endswith('?')
    out['has_flair'] = out['link_flair_text'].notna()

    age_hours = ((now - out['created_utc']).dt.total_seconds() / 3600).clip(lower=0)
    out['age_hours'] = age_hours.round(2)
    out['engagement_ratio'] = (out['num_comments'] / out['score'].clip(lower=1)).round(4)
    out['comments_per_hour'] = (out['num_comments'] / (age_hours + 1)).round(4)
    # Hacker-News style time-decayed ranking: young posts with high scores rank first
    out['hotness'] = (out['score'] / (age_hours + 2) ** 1.5).round(4)

    domain = out['domain'] if 'domain' in out.columns else pd.Series([None] * len(out), index=out.index)
    out['url_domain'] = out['url'].map(_url_domain)
    out['url_domain'] = out['url_domain'].where(~out['is_self'], 'self')
    out['url_domain'] = out['url_domain'].fillna(domain)

    searchable = out['title'].fillna('') + ' ' + out['link_flair_text'].fillna('')
    tag_matrix = {
        tag: searchable.str.contains(pattern, flags=re.IGNORECASE, regex=True)
        for tag, pattern in TOPIC_KEYWORDS.items()
    }
    tags_df = pd.DataFrame(tag_matrix, index=out.index)

    def _join_tags(row) -> str:
        return '|'.join(tag for tag, hit in row.items() if hit)

    out['tags'] = tags_df.apply(_join_tags, axis=1) if len(out) else pd.Series(dtype=str)
    out['primary_tag'] = out['tags'].map(lambda t: t.split('|')[0] if t else 'other')
    return out


def _atomic_write(path: str, writer) -> str:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp_path = f'{path}.tmp'
    try:
        writer(tmp_path)
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
    return path


def load_data_to_csv(df, file_path: str):
    path = file_path if file_path.endswith('.csv') else f'{file_path}.csv'
    _atomic_write(path, lambda tmp: df.to_csv(tmp, index=False))
    log.info('Wrote %d rows to %s', len(df), path)
    return path


def load_data_to_parquet(df, file_path: str) -> str:
    path = file_path if file_path.endswith('.parquet') else f'{file_path}.parquet'
    _atomic_write(path, lambda tmp: df.to_parquet(tmp, index=False, engine='pyarrow', compression='snappy'))
    log.info('Wrote %d rows to %s', len(df), path)
    return path
