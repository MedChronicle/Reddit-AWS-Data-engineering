from __future__ import annotations

import pandas as pd
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    text,
)
from sqlalchemy.dialects import postgresql, sqlite

from utils.constants import (
    BATCH_SIZE,
    DATABASE_HOST,
    DATABASE_NAME,
    DATABASE_PASSWORD,
    DATABASE_PORT,
    DATABASE_SCHEMA,
    DATABASE_USER,
)
from utils.logger import get_logger

log = get_logger(__name__)


def get_engine(url: str | None = None):
    if url is None:
        url = (f'postgresql+psycopg2://{DATABASE_USER}:{DATABASE_PASSWORD}'
               f'@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}')
    return create_engine(url, pool_pre_ping=True, future=True)


def build_tables(schema: str | None = DATABASE_SCHEMA):
    metadata = MetaData(schema=schema)
    posts = Table(
        'posts', metadata,
        Column('id', String(16), primary_key=True),
        Column('subreddit', String(100)),
        Column('title', Text),
        Column('score', Integer),
        Column('num_comments', Integer),
        Column('author', String(100)),
        Column('created_utc', DateTime),
        Column('url', Text),
        Column('permalink', Text),
        Column('over_18', Boolean),
        Column('edited', Boolean),
        Column('edited_at', DateTime),
        Column('spoiler', Boolean),
        Column('stickied', Boolean),
        Column('is_self', Boolean),
        Column('upvote_ratio', Float),
        Column('link_flair_text', String(200)),
        Column('url_domain', String(255)),
        Column('total_awards_received', Integer),
        Column('num_crossposts', Integer),
        Column('title_length', Integer),
        Column('title_word_count', Integer),
        Column('is_question', Boolean),
        Column('post_date', String(10)),
        Column('post_hour_utc', Integer),
        Column('post_weekday', String(10)),
        Column('age_hours', Float),
        Column('engagement_ratio', Float),
        Column('comments_per_hour', Float),
        Column('hotness', Float),
        Column('tags', Text),
        Column('primary_tag', String(50)),
        Column('first_seen_at', DateTime),
        Column('last_seen_at', DateTime),
    )
    snapshots = Table(
        'post_snapshots', metadata,
        Column('id', String(16), primary_key=True),
        Column('snapshot_date', String(10), primary_key=True),
        Column('score', Integer),
        Column('num_comments', Integer),
        Column('upvote_ratio', Float),
    )
    return metadata, posts, snapshots


def _views_sql(schema: str) -> list[str]:
    s = schema
    return [
        f"""CREATE OR REPLACE VIEW {s}.v_daily_top_posts AS
            SELECT * FROM (
                SELECT p.*, ROW_NUMBER() OVER (PARTITION BY post_date ORDER BY score DESC) AS day_rank
                FROM {s}.posts p) ranked
            WHERE day_rank <= 10""",
        f"""CREATE OR REPLACE VIEW {s}.v_author_leaderboard AS
            SELECT author, COUNT(*) AS posts, SUM(score) AS total_score,
                   ROUND(AVG(score)::numeric, 1) AS avg_score, SUM(num_comments) AS total_comments
            FROM {s}.posts WHERE author <> '[deleted]'
            GROUP BY author ORDER BY total_score DESC""",
        f"""CREATE OR REPLACE VIEW {s}.v_tag_trends AS
            SELECT post_date, primary_tag, COUNT(*) AS posts,
                   ROUND(AVG(score)::numeric, 1) AS avg_score, SUM(num_comments) AS comments
            FROM {s}.posts GROUP BY post_date, primary_tag ORDER BY post_date DESC, posts DESC""",
        f"""CREATE OR REPLACE VIEW {s}.v_score_velocity AS
            SELECT id, snapshot_date, score,
                   score - LAG(score) OVER (PARTITION BY id ORDER BY snapshot_date) AS score_change,
                   num_comments - LAG(num_comments) OVER (PARTITION BY id ORDER BY snapshot_date) AS comments_change
            FROM {s}.post_snapshots""",
    ]


def _to_records(df: pd.DataFrame, columns: list[str]) -> list[dict]:
    subset = df[[c for c in columns if c in df.columns]].copy()
    for column in subset.columns:
        if pd.api.types.is_datetime64_any_dtype(subset[column]):
            subset[column] = subset[column].astype(object).where(subset[column].notna(), None)
            subset[column] = subset[column].map(lambda v: v.to_pydatetime() if v is not None else None)
    subset = subset.astype(object).where(subset.notna(), None)
    records = subset.to_dict(orient='records')
    for record in records:
        for key, value in record.items():
            if hasattr(value, 'item') and not isinstance(value, (str, bytes)):
                record[key] = value.item()  # numpy scalar -> python scalar
    return records


def _upsert(conn, table: Table, records: list[dict], key_columns: list[str],
            do_not_update: tuple = ()) -> None:
    if not records:
        return
    dialect = conn.dialect.name
    insert = {'postgresql': postgresql.insert, 'sqlite': sqlite.insert}.get(dialect)
    if insert is None:
        raise NotImplementedError(f'Upsert not implemented for dialect {dialect}')
    columns = list(records[0].keys())
    for start in range(0, len(records), BATCH_SIZE):
        chunk = records[start:start + BATCH_SIZE]
        stmt = insert(table)
        update_cols = {c: stmt.excluded[c] for c in columns if c not in key_columns and c not in do_not_update}
        stmt = stmt.on_conflict_do_update(index_elements=key_columns, set_=update_cols)
        conn.execute(stmt, chunk)


def load_posts_to_postgres(df: pd.DataFrame, engine=None, schema: str | None = DATABASE_SCHEMA,
                           snapshot_date: str | None = None, now=None) -> dict:
    if df.empty:
        log.warning('No rows to load into Postgres')
        return {'posts': 0, 'snapshots': 0}

    engine = engine or get_engine()
    is_postgres = engine.dialect.name == 'postgresql'
    schema = schema if is_postgres else None  # sqlite has no schemas
    _, posts, snapshots = build_tables(schema)
    stamp = pd.Timestamp(now) if now is not None else pd.Timestamp.now(tz='UTC').tz_localize(None)

    frame = df.copy()
    frame['first_seen_at'] = stamp
    frame['last_seen_at'] = stamp
    snapshot_day = snapshot_date or stamp.strftime('%Y-%m-%d')
    snapshot_frame = frame.assign(snapshot_date=snapshot_day)

    post_columns = [c.name for c in posts.columns]
    snapshot_columns = [c.name for c in snapshots.columns]

    with engine.begin() as conn:
        if is_postgres and schema:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema}'))
        posts.metadata.create_all(conn)
        _upsert(conn, posts, _to_records(frame, post_columns), ['id'], do_not_update=('first_seen_at',))
        _upsert(conn, snapshots, _to_records(snapshot_frame, snapshot_columns), ['id', 'snapshot_date'])
        if is_postgres:
            for statement in _views_sql(schema):
                conn.execute(text(statement))

    log.info('Upserted %d posts and %d snapshots into %s', len(frame), len(frame),
             f'{schema}.posts' if schema else 'posts')
    return {'posts': len(frame), 'snapshots': len(frame)}
