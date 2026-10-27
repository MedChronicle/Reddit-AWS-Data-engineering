"""Full chain: fake Reddit -> files -> S3 (moto) -> Postgres (sqlite), wired through fake XCom."""
import json

import pandas as pd
import pytest
from sqlalchemy import text

import etls.aws_etl as aws
import etls.postgres_etl as pg
import pipelines.aws_s3_pipeline as s3_pipe
import pipelines.reddit_pipeline as rp
from tests.fakes import FakeReddit, FakeTI, free_port


@pytest.fixture
def env(monkeypatch, tmp_path):
    pytest.importorskip('moto')
    from moto.server import ThreadedMotoServer
    port = free_port()
    server = ThreadedMotoServer(ip_address='127.0.0.1', port=port, verbose=False)
    server.start()

    monkeypatch.setattr(rp, 'OUTPUT_PATH', str(tmp_path / 'out'))
    monkeypatch.setattr(rp, 'connect_reddit', lambda: FakeReddit(8))
    monkeypatch.setattr(aws, 'AWS_ENDPOINT_URL', f'http://127.0.0.1:{port}')
    for name, value in dict(AWS_ACCESS_KEY_ID='t', AWS_ACCESS_KEY='t', AWS_REGION='us-east-1', AWS_SESSION_TOKEN='').items():
        monkeypatch.setattr(aws, name, value)
    monkeypatch.setattr(s3_pipe, 'AWS_BUCKET_NAME', 'e2e-bucket')
    engine = pg.get_engine(f'sqlite:///{tmp_path}/e2e.db')
    monkeypatch.setattr(pg, 'get_engine', lambda url=None: engine)
    yield tmp_path, engine
    server.stop()


def test_whole_pipeline(env):
    tmp_path, engine = env
    ti = FakeTI()

    path = rp.reddit_pipeline('reddit_dataengineering_20241019', 'dataengineering', 'day', '100', ti=ti, ds='2024-10-19')
    ti.xcom_push('return_value', path)           # what Airflow does with the return value

    # ---- local files ----
    assert path.endswith('reddit_dataengineering_20241019.csv') and not path.endswith('.csv.csv')
    df = pd.read_csv(path)
    assert len(df) == 8 and {'hotness', 'primary_tag', 'edited_at'} <= set(df.columns)
    summary = json.load(open(ti.store[('run_reddit_pipeline', 'summary_path')]))
    assert summary['rows'] == 8 and summary['quality']['passed'] is True
    assert ti.store[('run_reddit_pipeline', 'row_count')] == 8

    # ---- S3 ----
    uploaded = s3_pipe.upload_s3_pipeline(ti)
    base = 's3://e2e-bucket/{}/subreddit=dataengineering/dt=2024-10-19/reddit_dataengineering_20241019'
    assert uploaded == {'csv': base.format('raw') + '.csv',
                        'parquet': base.format('processed') + '.parquet',
                        'summary': base.format('reports') + '_summary.json'}
    s3 = aws.connect_to_s3()
    assert all(s3.exists(u.replace('s3://', '')) for u in uploaded.values())

    # ---- Postgres (sqlite stand-in) ----
    from pipelines.postgres_pipeline import load_postgres_pipeline
    assert load_postgres_pipeline(ti) == {'posts': 8, 'snapshots': 8}
    load_postgres_pipeline(ti)                    # retry/rerun of the task must not duplicate
    with engine.connect() as c:
        assert c.execute(text('select count(*) from posts')).scalar() == 8


def test_upload_fails_loudly_when_extract_produced_nothing(env):
    with pytest.raises(ValueError, match='No file path in XCom'):
        s3_pipe.upload_s3_pipeline(FakeTI())      # old code: AttributeError on None.split


def test_csv_fallback_when_parquet_disabled(env, monkeypatch):
    monkeypatch.setattr(rp, 'WRITE_PARQUET', False)
    ti = FakeTI()
    ti.xcom_push('return_value', rp.reddit_pipeline('f', 'dataengineering', ti=ti))
    assert ti.store[('run_reddit_pipeline', 'parquet_path')] is None
    from pipelines.postgres_pipeline import load_postgres_pipeline
    assert load_postgres_pipeline(ti)['posts'] == 8
    assert set(s3_pipe.upload_s3_pipeline(ti)) == {'csv', 'summary'}


def test_limit_coercion_and_quality_abort(env, monkeypatch):
    assert rp._coerce_limit('None') is None and rp._coerce_limit('25') == 25 and rp._coerce_limit(None) is None
    monkeypatch.setattr(rp, 'connect_reddit', lambda: FakeReddit(0))
    from etls.data_quality import DataQualityError
    with pytest.raises(DataQualityError):         # empty extraction must fail the task, not upload an empty file
        rp.reddit_pipeline('f', 'dataengineering')
