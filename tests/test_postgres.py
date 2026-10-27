import pandas as pd
from sqlalchemy import text

from etls.postgres_etl import get_engine, load_posts_to_postgres
from etls.reddit_etl import enrich_features, transform_data
from tests.conftest import make_raw_post


def _df(raw):
    return enrich_features(transform_data(raw), reference_time=pd.Timestamp('2024-10-20'))


def test_upsert_is_idempotent_and_updates(tmp_path, raw_posts):
    engine = get_engine(f'sqlite:///{tmp_path}/t.db')
    t0, t1 = pd.Timestamp('2024-10-19 10:00'), pd.Timestamp('2024-10-20 10:00')

    assert load_posts_to_postgres(_df(raw_posts), engine=engine, now=t0)['posts'] == 5
    load_posts_to_postgres(_df(raw_posts), engine=engine, now=t0)           # re-run: no duplicates
    with engine.connect() as c:
        assert c.execute(text('select count(*) from posts')).scalar() == 5
        assert c.execute(text('select count(*) from post_snapshots')).scalar() == 5

    changed = [make_raw_post(i) for i in range(5)]
    changed[0]['score'] = 5000
    load_posts_to_postgres(_df(changed), engine=engine, now=t1)             # next day
    with engine.connect() as c:
        row = c.execute(text("select score, first_seen_at, last_seen_at from posts where id='p0'")).one()
        assert row[0] == 5000
        assert str(row[1]).startswith('2024-10-19') and str(row[2]).startswith('2024-10-20')  # first_seen kept
        assert c.execute(text('select count(*) from posts')).scalar() == 5
        assert c.execute(text('select count(*) from post_snapshots')).scalar() == 10  # one per day
        scores = [r[0] for r in c.execute(text("select score from post_snapshots where id='p0' order by snapshot_date"))]
        assert scores == [100, 5000]


def test_nulls_and_types_roundtrip(tmp_path, raw_posts):
    engine = get_engine(f'sqlite:///{tmp_path}/t.db')
    load_posts_to_postgres(_df(raw_posts), engine=engine, now=pd.Timestamp('2024-10-19'))
    with engine.connect() as c:
        row = c.execute(text("select edited_at, link_flair_text, over_18 from posts where id='p1'")).one()
        assert row[0] is None and row[1] == 'Career' and row[2] == 0


def test_empty_frame_is_noop(tmp_path):
    engine = get_engine(f'sqlite:///{tmp_path}/t.db')
    assert load_posts_to_postgres(pd.DataFrame(), engine=engine) == {'posts': 0, 'snapshots': 0}
