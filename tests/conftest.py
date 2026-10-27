import os
import sys
from types import SimpleNamespace

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
# Make sure tests never pick up a developer's real config.conf
os.environ.setdefault('PIPELINE_CONFIG', os.path.join(ROOT, 'tests', 'does-not-exist.conf'))


def make_raw_post(i: int, **overrides):
    """A raw post shaped like praw's ``vars(submission)`` output."""
    post = {
        'id': f'p{i}',
        'title': f'Spark vs Snowflake for pipeline #{i}?' if i % 2 == 0 else f'My salary story {i}',
        'score': 100 - i * 10,
        'num_comments': i * 3,
        'author': SimpleNamespace(name=f'user{i}') if False else f'user{i}',
        'created_utc': 1729260986.0 + i * 3600,
        'url': f'https://example.com/{i}' if i % 2 else f'https://www.reddit.com/r/dataengineering/comments/p{i}/',
        'over_18': False,
        'edited': 1729283568.0 if i == 2 else False,
        'spoiler': False,
        'stickied': False,
        'subreddit': 'dataengineering',
        'upvote_ratio': 0.9,
        'link_flair_text': 'Career' if i % 2 else None,
        'domain': 'example.com' if i % 2 else 'self.dataengineering',
        'is_self': i % 2 == 0,
        'permalink': f'/r/dataengineering/comments/p{i}/x/',
        'total_awards_received': 0,
        'num_crossposts': 0,
        'selftext': 'body ' * 10,
    }
    post.update(overrides)
    return post


@pytest.fixture
def raw_posts():
    return [make_raw_post(i) for i in range(5)]
