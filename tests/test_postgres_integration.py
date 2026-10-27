"""Runs against a REAL Postgres. Skipped unless TEST_DATABASE_URL is set, e.g.

    TEST_DATABASE_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/airflow_reddit pytest tests/test_postgres_integration.py

CI starts a Postgres service container and sets this variable automatically.
"""
import os

import pandas as pd
import pytest
from sqlalchemy import text

from etls.postgres_etl import get_engine, load_posts_to_postgres
from etls.reddit_etl import enrich_features, transform_data
from tests.conftest import make_raw_post

URL = os.environ.get('TEST_DATABASE_URL')
pytestmark = pytest.mark.skipif(not URL, reason='TEST_DATABASE_URL not set')

SCHEMA = 'reddit_it'


@pytest.fixture
def engine():
    engine = get_engine(URL)
    with engine.begin() as c:
        c.execute(text(f'DROP SCHEMA IF EXISTS {SCHEMA} CASCADE'))
    yield engine
    with engine.begin() as c:
        c.execute(text(f'DROP SCHEMA IF EXISTS {SCHEMA} CASCADE'))


def _df(raw):
    return enrich_features(transform_data(raw), reference_time=pd.Timestamp('2024-10-20'))


def test_upsert_and_views_on_real_postgres(engine):
    raw = [make_raw_post(i) for i in range(5)]
    load_posts_to_postgres(_df(raw), engine=engine, schema=SCHEMA, now=pd.Timestamp('2024-10-19 10:00'))
    load_posts_to_postgres(_df(raw), engine=engine, schema=SCHEMA, now=pd.Timestamp('2024-10-19 11:00'))  # rerun same day
    raw[0]['score'] = 5000
    load_posts_to_postgres(_df(raw), engine=engine, schema=SCHEMA, now=pd.Timestamp('2024-10-20 10:00'))  # next day

    with engine.connect() as c:
        assert c.execute(text(f'select count(*) from {SCHEMA}.posts')).scalar() == 5
        assert c.execute(text(f'select count(*) from {SCHEMA}.post_snapshots')).scalar() == 10
        row = c.execute(text(f"select score, first_seen_at, last_seen_at, edited from {SCHEMA}.posts where id='p0'")).one()
        assert row[0] == 5000 and row[1].day == 19 and row[2].day == 20 and row[3] is False
        assert c.execute(text(f"select edited from {SCHEMA}.posts where id='p2'")).scalar() is True

        # views
        assert c.execute(text(f'select count(*) from {SCHEMA}.v_daily_top_posts')).scalar() == 5
        assert c.execute(text(f'select count(*) from {SCHEMA}.v_author_leaderboard')).scalar() == 5
        assert c.execute(text(f'select sum(posts) from {SCHEMA}.v_tag_trends')).scalar() == 5
        change = c.execute(text(
            f"select score_change from {SCHEMA}.v_score_velocity where id='p0' and snapshot_date='2024-10-20'")).scalar()
        assert change == 4900
