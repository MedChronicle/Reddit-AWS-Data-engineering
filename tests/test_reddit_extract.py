import pytest
import tenacity

import etls.reddit_etl as r
from tests.fakes import FakeReddit


def test_extract_posts_returns_all_fields_and_respects_limit():
    posts = r.extract_posts(FakeReddit(5), 'dataengineering', 'day', 3)
    assert len(posts) == 3
    assert set(posts[0]) == set(r.ALL_POST_FIELDS)


def test_extract_tolerates_missing_fields():
    class Sparse(FakeReddit):
        def __init__(self):
            super().__init__(1)
            self.sub.posts = [{'id': 'x', 'title': 't'}]      # Reddit omitted most fields
    posts = r.extract_posts(Sparse(), 'x')
    assert posts[0]['id'] == 'x' and posts[0]['score'] is None


def test_transient_errors_are_retried(monkeypatch):
    monkeypatch.setattr(r._fetch_posts.retry, 'wait', tenacity.wait_none())
    reddit = FakeReddit(2, failures=2)
    assert len(r.extract_posts(reddit, 'x')) == 2
    assert reddit.sub.calls == 3


def test_gives_up_after_max_attempts(monkeypatch):
    import prawcore
    monkeypatch.setattr(r._fetch_posts.retry, 'wait', tenacity.wait_none())
    reddit = FakeReddit(2, failures=99)
    with pytest.raises(prawcore.exceptions.RequestException):
        r.extract_posts(reddit, 'x')
    assert reddit.sub.calls == 4


def test_connect_without_credentials_raises_instead_of_exiting(monkeypatch):
    monkeypatch.setattr(r, 'CLIENT_ID', '')
    monkeypatch.setattr(r, 'SECRET', '')
    with pytest.raises(r.ConfigError):      # the old code called sys.exit(1)
        r.connect_reddit()


def test_connect_validates_credentials(monkeypatch):
    seen = {}

    class FakeAuth:
        def scopes(self):
            seen['checked'] = True
            return {'*'}

    class FakePraw:
        def __init__(self, **kw):
            seen.update(kw)
            self.auth = FakeAuth()

    monkeypatch.setattr(r, 'CLIENT_ID', 'id')
    monkeypatch.setattr(r, 'SECRET', 'secret')
    monkeypatch.setattr(r.praw, 'Reddit', FakePraw)
    r.connect_reddit()
    assert seen['checked'] and seen['client_id'] == 'id' and 'Airscholar' not in seen['user_agent']
