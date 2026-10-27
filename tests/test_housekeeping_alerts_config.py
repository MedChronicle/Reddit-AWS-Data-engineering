import importlib
import os
import time

from etls.housekeeping import cleanup_old_files


def test_cleanup_removes_only_old_matching_files(tmp_path):
    old, new, other = tmp_path / 'a.csv', tmp_path / 'b.csv', tmp_path / 'keep.txt'
    for f in (old, new, other):
        f.write_text('x')
    past = time.time() - 20 * 86400
    os.utime(old, (past, past))
    os.utime(other, (past, past))
    removed = cleanup_old_files(str(tmp_path), 14)
    assert removed == [str(old)] and new.exists() and other.exists()
    assert cleanup_old_files(str(tmp_path), 0) == []            # 0 disables cleanup
    assert cleanup_old_files(str(tmp_path / 'missing'), 14) == []


def test_alerts_without_webhook_never_raise(monkeypatch):
    import utils.alerts as alerts
    monkeypatch.setattr(alerts, 'SLACK_WEBHOOK_URL', '')
    alerts.notify_failure({'run_id': 'r', 'exception': 'boom'})
    alerts.notify_success({'run_id': 'r'})


def test_alerts_post_to_slack_and_survive_errors(monkeypatch):
    import utils.alerts as alerts
    calls = []
    monkeypatch.setattr(alerts, 'SLACK_WEBHOOK_URL', 'http://hook')
    monkeypatch.setattr(alerts.requests, 'post', lambda url, json, timeout: calls.append(json) or type('R', (), {'raise_for_status': lambda s: None})())
    alerts.notify_failure({'run_id': 'r', 'exception': 'boom'})
    assert 'boom' in calls[0]['text']

    def explode(*a, **k):
        raise RuntimeError('network down')
    monkeypatch.setattr(alerts.requests, 'post', explode)
    alerts.notify_failure({'run_id': 'r', 'exception': 'boom'})   # must not raise


def test_config_env_override_and_placeholders(monkeypatch, tmp_path):
    conf = tmp_path / 'c.conf'
    conf.write_text('[aws]\naws_access_key_id = [aws access key id]\naws_bucket_name = from-file\n'
                    '[database]\ndatabase_password = pa%ss\n[etl_settings]\nsubreddits = a, b ,c\nbatch_size = 7\n')
    monkeypatch.setenv('PIPELINE_CONFIG', str(conf))
    monkeypatch.setenv('AWS_REGION', 'ap-south-1')
    for var in ('AWS_ACCESS_KEY_ID', 'AWS_BUCKET_NAME', 'DATABASE_PASSWORD', 'ETL_SUBREDDITS', 'ETL_BATCH_SIZE'):
        monkeypatch.delenv(var, raising=False)
    import utils.constants as c
    c = importlib.reload(c)
    try:
        assert c.AWS_ACCESS_KEY_ID == ''            # placeholder ignored -> default credential chain
        assert c.AWS_BUCKET_NAME == 'from-file'
        assert c.AWS_REGION == 'ap-south-1'         # env wins
        assert c.DATABASE_PASSWORD == 'pa%ss'       # '%' no longer breaks configparser
        assert c.SUBREDDITS == ['a', 'b', 'c'] and c.BATCH_SIZE == 7
    finally:
        monkeypatch.delenv('PIPELINE_CONFIG')
        monkeypatch.delenv('AWS_REGION')
        importlib.reload(c)


def test_original_constant_names_still_exist():
    import utils.constants as c
    for name in ('SECRET', 'CLIENT_ID', 'DATABASE_HOST', 'DATABASE_NAME', 'DATABASE_PORT', 'DATABASE_USER',
                 'DATABASE_PASSWORD', 'AWS_ACCESS_KEY_ID', 'AWS_ACCESS_KEY', 'AWS_REGION', 'AWS_BUCKET_NAME',
                 'INPUT_PATH', 'OUTPUT_PATH', 'POST_FIELDS'):
        assert hasattr(c, name), name
