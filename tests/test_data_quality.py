import pandas as pd
import pytest

from etls.data_quality import DataQualityError, validate_posts
from etls.reddit_etl import enrich_features, transform_data

NOW = pd.Timestamp('2024-10-20')


def frame(raw_posts):
    return enrich_features(transform_data(raw_posts), reference_time=NOW)


def test_clean_data_passes(raw_posts):
    df, report = validate_posts(frame(raw_posts), error_handling='abort', now=NOW)
    assert report.passed and report.rows_out == report.rows_in == 5 and not report.warnings


def test_missing_columns_abort(raw_posts):
    with pytest.raises(DataQualityError, match='missing required columns'):
        validate_posts(frame(raw_posts).drop(columns=['score']), error_handling='abort', now=NOW)


def test_too_few_rows_abort():
    with pytest.raises(DataQualityError, match='expected at least'):
        validate_posts(frame([]), error_handling='abort', min_rows=1, now=NOW)


def test_bad_rows_are_dropped_in_continue_mode(raw_posts):
    df = frame(raw_posts)
    df.loc[0, 'id'] = ''                       # blank id
    df.loc[1, 'created_utc'] = pd.Timestamp('2030-01-01')   # future
    df.loc[3, 'id'] = df.loc[2, 'id']          # duplicate
    clean, report = validate_posts(df, error_handling='continue', now=NOW)
    assert not report.passed and report.rows_in == 5 and report.rows_out == 2
    assert len(report.errors) == 3
    with pytest.raises(DataQualityError):
        validate_posts(df, error_handling='abort', now=NOW)


def test_warnings_do_not_fail(raw_posts):
    df = frame(raw_posts)
    df.loc[0, 'score'] = -5
    df.loc[1, 'upvote_ratio'] = 1.7
    clean, report = validate_posts(df, error_handling='abort', now=NOW)
    assert report.passed and len(report.warnings) == 2 and len(clean) == 5
