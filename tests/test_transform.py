import numpy as np
import pandas as pd

from etls.reddit_etl import enrich_features, load_data_to_csv, load_data_to_parquet, transform_data
from tests.conftest import make_raw_post
from utils.constants import POST_FIELDS


def test_original_columns_come_first_in_original_order(raw_posts):
    df = transform_data(raw_posts)
    assert tuple(df.columns[:len(POST_FIELDS)]) == POST_FIELDS


def test_types(raw_posts):
    df = transform_data(raw_posts)
    assert pd.api.types.is_datetime64_any_dtype(df['created_utc'])
    assert df['edited'].dtype == bool and df['over_18'].dtype == bool
    assert df['score'].dtype.kind == 'i' and df['num_comments'].dtype.kind == 'i'


def test_edited_posts_are_flagged_and_keep_timestamp(raw_posts):
    """Regression: the old code replaced edit timestamps with the mode (False)."""
    df = transform_data(raw_posts).set_index('id')
    assert bool(df.loc['p2', 'edited']) is True
    assert df.loc['p2', 'edited_at'] == pd.Timestamp('2024-10-18 20:32:48')
    assert bool(df.loc['p1', 'edited']) is False
    assert pd.isna(df.loc['p1', 'edited_at'])


def test_deleted_author_and_duplicates_and_sorting():
    posts = [make_raw_post(1, author=None), make_raw_post(2), make_raw_post(2, score=999)]
    df = transform_data(posts)
    assert len(df) == 2
    assert df.iloc[0]['id'] == 'p2' and df.iloc[0]['score'] == 999  # best duplicate kept, sorted first
    assert df.set_index('id').loc['p1', 'author'] == '[deleted]'


def test_works_with_only_original_fields():
    """Old callers that only provide the original 11 fields must still work."""
    posts = [{k: v for k, v in make_raw_post(i).items() if k in POST_FIELDS} for i in range(3)]
    df = enrich_features(transform_data(posts))
    assert len(df) == 3 and (df['primary_tag'].notna()).all()


def test_enrich_features_is_deterministic_and_correct(raw_posts):
    now = pd.Timestamp('2024-10-19 00:00:00')
    df = enrich_features(transform_data(raw_posts), reference_time=now).set_index('id')
    assert df.loc['p0', 'is_question'] and not df.loc['p1', 'is_question']
    assert df.loc['p0', 'primary_tag'] in {'spark', 'warehouse'}
    assert 'spark' in df.loc['p0', 'tags'].split('|') and 'warehouse' in df.loc['p0', 'tags'].split('|')
    assert df.loc['p1', 'primary_tag'] == 'career'
    assert df.loc['p0', 'post_hour_utc'] == 14 and df.loc['p0', 'post_weekday'] == 'Friday'
    assert df.loc['p0', 'title_word_count'] == len(df.loc['p0', 'title'].split())
    assert df.loc['p0', 'url_domain'] == 'self' and df.loc['p1', 'url_domain'] == 'example.com'
    assert (df['age_hours'] >= 0).all()
    expected = 100 / (df.loc['p0', 'age_hours'] + 2) ** 1.5
    assert np.isclose(df.loc['p0', 'hotness'], expected, rtol=1e-2)


def test_enrich_future_posts_clip_age_to_zero():
    df = enrich_features(transform_data([make_raw_post(0)]), reference_time=pd.Timestamp('2000-01-01'))
    assert df.loc[0, 'age_hours'] == 0


def test_empty_input_does_not_crash():
    df = enrich_features(transform_data([]))
    assert df.empty


def test_csv_extension_not_doubled_and_no_index(tmp_path, raw_posts):
    df = transform_data(raw_posts)
    path = load_data_to_csv(df, str(tmp_path / 'reddit_x.csv'))
    assert path.endswith('reddit_x.csv') and not path.endswith('.csv.csv')
    assert load_data_to_csv(df, str(tmp_path / 'other')).endswith('other.csv')
    header = open(path).readline()
    assert not header.startswith(',')          # no unnamed index column
    assert not list(tmp_path.glob('*.tmp'))    # atomic write left nothing behind


def test_parquet_roundtrip(tmp_path, raw_posts):
    df = enrich_features(transform_data(raw_posts))
    path = load_data_to_parquet(df, str(tmp_path / 'reddit_x'))
    back = pd.read_parquet(path)
    assert path.endswith('.parquet') and len(back) == len(df)
    assert list(back.columns) == list(df.columns)
