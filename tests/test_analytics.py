import json

import pandas as pd

from etls.analytics_etl import build_summary, write_summary
from etls.reddit_etl import enrich_features, transform_data


def test_summary_values(raw_posts, tmp_path):
    df = enrich_features(transform_data(raw_posts), reference_time=pd.Timestamp('2024-10-20'))
    s = build_summary(df, subreddit='dataengineering', run_date='2024-10-19')
    assert s['rows'] == 5 and s['total_score'] == 100 + 90 + 80 + 70 + 60
    assert s['top_posts'][0]['id'] == 'p0'
    assert sum(s['posts_by_tag'].values()) == 5
    path = write_summary(s, str(tmp_path / 'sub' / 's.json'))
    assert json.load(open(path))['rows'] == 5   # JSON-serialisable and directories created


def test_summary_empty():
    assert build_summary(pd.DataFrame(), 'x', 'd')['rows'] == 0
