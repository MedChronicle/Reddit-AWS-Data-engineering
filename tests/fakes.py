"""Small fakes shared by the tests (no network, no Airflow needed)."""
import socket
from types import SimpleNamespace

from tests.conftest import make_raw_post


class FakeTI:
    """Mimics the bits of an Airflow TaskInstance the pipelines use."""
    def __init__(self, store=None, task_id='run_reddit_pipeline', ds='2024-10-19'):
        self.store = store if store is not None else {}
        self.task_id = task_id
        self.ds = ds

    def xcom_push(self, key, value):
        self.store[(self.task_id, key)] = value

    def xcom_pull(self, task_ids=None, key='return_value'):
        return self.store.get((task_ids, key))


class FakeSubreddit:
    def __init__(self, posts, failures=0):
        self.posts, self.failures, self.calls = posts, failures, 0

    def top(self, time_filter='day', limit=None):
        self.calls += 1
        if self.calls <= self.failures:
            import prawcore
            raise prawcore.exceptions.RequestException(Exception('boom'), (), {})
        for post in self.posts[:limit]:
            yield SimpleNamespace(**post)


class FakeReddit:
    def __init__(self, n=5, failures=0):
        self.sub = FakeSubreddit([make_raw_post(i) for i in range(n)], failures)

    def subreddit(self, name):
        return self.sub


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]
