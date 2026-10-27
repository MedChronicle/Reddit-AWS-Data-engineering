import pytest

import etls.aws_etl as aws
from tests.fakes import free_port


@pytest.fixture(scope='module')
def moto_endpoint():
    pytest.importorskip('moto')
    from moto.server import ThreadedMotoServer
    port = free_port()
    server = ThreadedMotoServer(ip_address='127.0.0.1', port=port, verbose=False)
    server.start()
    yield f'http://127.0.0.1:{port}'
    server.stop()


@pytest.fixture
def s3_env(monkeypatch, moto_endpoint):
    monkeypatch.setattr(aws, 'AWS_ENDPOINT_URL', moto_endpoint)
    monkeypatch.setattr(aws, 'AWS_ACCESS_KEY_ID', 'test')
    monkeypatch.setattr(aws, 'AWS_ACCESS_KEY', 'test')
    monkeypatch.setattr(aws, 'AWS_REGION', 'us-east-1')
    monkeypatch.setattr(aws, 'AWS_SESSION_TOKEN', '')
    monkeypatch.setattr(aws, 'AWS_HARDEN_BUCKET', True)
    return aws


def test_build_s3_key_partitions():
    assert aws.build_s3_key('raw', 'f.csv', 'dataengineering', '2024-10-19') == \
        'raw/subreddit=dataengineering/dt=2024-10-19/f.csv'
    assert aws.build_s3_key('', 'f.csv') == 'f.csv'


def test_bucket_create_harden_upload_verify(s3_env, tmp_path):
    s3 = aws.connect_to_s3()
    assert aws.create_bucket_if_not_exist(s3, 'test-bucket') is True
    assert aws.create_bucket_if_not_exist(s3, 'test-bucket') is False      # idempotent

    client = aws._boto_client()
    assert client.get_public_access_block(Bucket='test-bucket')['PublicAccessBlockConfiguration']['BlockPublicAcls']
    assert client.get_bucket_versioning(Bucket='test-bucket')['Status'] == 'Enabled'
    rule = client.get_bucket_encryption(Bucket='test-bucket')['ServerSideEncryptionConfiguration']['Rules'][0]
    assert rule['ApplyServerSideEncryptionByDefault']['SSEAlgorithm'] == 'AES256'
    assert client.get_bucket_lifecycle_configuration(Bucket='test-bucket')['Rules'][0]['ID'] == 'raw-to-infrequent-access'

    f = tmp_path / 'a.csv'
    f.write_text('id\n1\n')
    uri = aws.upload_to_s3(s3, str(f), 'test-bucket', 'a.csv')              # original call signature
    assert uri == 's3://test-bucket/raw/a.csv'
    uri2 = aws.upload_to_s3(s3, str(f), 'test-bucket', 'subreddit=x/a.csv', prefix='processed')
    assert uri2 == 's3://test-bucket/processed/subreddit=x/a.csv'
    assert s3.cat('test-bucket/raw/a.csv') == b'id\n1\n'


def test_missing_file_raises(s3_env):
    with pytest.raises(FileNotFoundError):
        aws.upload_to_s3(aws.connect_to_s3(), '/nope/missing.csv', 'test-bucket', 'x.csv')


def test_empty_bucket_name_is_rejected(s3_env):
    with pytest.raises(ValueError):
        aws.create_bucket_if_not_exist(aws.connect_to_s3(), '')


def test_hardening_failure_only_warns(s3_env, monkeypatch):
    class Denied:
        def __getattr__(self, name):
            def call(**kw):
                from botocore.exceptions import ClientError
                raise ClientError({'Error': {'Code': 'AccessDenied'}}, name)
            return call
    monkeypatch.setattr(aws, '_boto_client', lambda: Denied())
    assert aws.harden_bucket('b') == {'public_access_block': False, 'encryption': False,
                                       'versioning': False, 'lifecycle': False}
