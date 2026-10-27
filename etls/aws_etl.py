from __future__ import annotations

import os

import botocore.session
import s3fs
from botocore.exceptions import ClientError

from utils.constants import (
    AWS_ACCESS_KEY,
    AWS_ACCESS_KEY_ID,
    AWS_ENDPOINT_URL,
    AWS_HARDEN_BUCKET,
    AWS_REGION,
    AWS_SESSION_TOKEN,
)
from utils.logger import get_logger

log = get_logger(__name__)


def _client_kwargs() -> dict:
    kwargs = {}
    if AWS_REGION:
        kwargs['region_name'] = AWS_REGION
    if AWS_ENDPOINT_URL:
        kwargs['endpoint_url'] = AWS_ENDPOINT_URL
    return kwargs


def connect_to_s3():
    kwargs = {'anon': False, 'client_kwargs': _client_kwargs(), 'skip_instance_cache': True}
    if AWS_ACCESS_KEY_ID and AWS_ACCESS_KEY:
        kwargs.update(key=AWS_ACCESS_KEY_ID, secret=AWS_ACCESS_KEY)
        if AWS_SESSION_TOKEN:
            kwargs['token'] = AWS_SESSION_TOKEN
    try:
        return s3fs.S3FileSystem(**kwargs)
    except Exception:
        log.exception('Could not create the S3 filesystem')
        raise


def _boto_client():
    session = botocore.session.get_session()
    kwargs = _client_kwargs()
    if AWS_ACCESS_KEY_ID and AWS_ACCESS_KEY:
        kwargs.update(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_ACCESS_KEY)
        if AWS_SESSION_TOKEN:
            kwargs['aws_session_token'] = AWS_SESSION_TOKEN
    return session.create_client('s3', **kwargs)


def _create_bucket(bucket: str) -> None:
    kwargs = {'Bucket': bucket}
    if AWS_REGION and AWS_REGION != 'us-east-1':
        kwargs['CreateBucketConfiguration'] = {'LocationConstraint': AWS_REGION}
    _boto_client().create_bucket(**kwargs)


def create_bucket_if_not_exist(s3: s3fs.S3FileSystem, bucket: str):
    if not bucket:
        raise ValueError('aws_bucket_name is empty - set AWS_BUCKET_NAME or [aws] aws_bucket_name')
    try:
        if not s3.exists(bucket):
            _create_bucket(bucket)
            s3.invalidate_cache()
            log.info('Bucket %s created', bucket)
            created = True
        else:
            log.info('Bucket %s already exists', bucket)
            created = False
    except Exception:
        log.exception('Could not check/create bucket %s', bucket)
        raise
    if AWS_HARDEN_BUCKET:
        harden_bucket(bucket)
    return created


def harden_bucket(bucket: str) -> dict:
    client = _boto_client()
    applied = {}
    steps = {
        'public_access_block': lambda: client.put_public_access_block(
            Bucket=bucket,
            PublicAccessBlockConfiguration={
                'BlockPublicAcls': True, 'IgnorePublicAcls': True,
                'BlockPublicPolicy': True, 'RestrictPublicBuckets': True,
            }),
        'encryption': lambda: client.put_bucket_encryption(
            Bucket=bucket,
            ServerSideEncryptionConfiguration={
                'Rules': [{'ApplyServerSideEncryptionByDefault': {'SSEAlgorithm': 'AES256'}}]}),
        'versioning': lambda: client.put_bucket_versioning(
            Bucket=bucket, VersioningConfiguration={'Status': 'Enabled'}),
        'lifecycle': lambda: client.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={'Rules': [{
                'ID': 'raw-to-infrequent-access',
                'Status': 'Enabled',
                'Filter': {'Prefix': 'raw/'},
                'Transitions': [{'Days': 30, 'StorageClass': 'STANDARD_IA'}],
                'NoncurrentVersionExpiration': {'NoncurrentDays': 30},
                'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 7},
            }]}),
    }
    for name, call in steps.items():
        try:
            call()
            applied[name] = True
        except ClientError as exc:
            applied[name] = False
            log.warning('Bucket hardening step %s skipped: %s', name, exc.response['Error'].get('Code'))
        except Exception as exc:  # noqa: BLE001 - e.g. a provider without that API
            applied[name] = False
            log.warning('Bucket hardening step %s skipped: %s', name, exc)
    return applied


def build_s3_key(prefix: str, file_name: str, subreddit: str | None = None, run_date: str | None = None) -> str:
    parts = [prefix.strip('/')] if prefix and prefix.strip('/') else []
    if subreddit:
        parts.append(f'subreddit={subreddit}')
    if run_date:
        parts.append(f'dt={run_date}')
    parts.append(file_name)
    return '/'.join(parts)


def upload_to_s3(s3: s3fs.S3FileSystem, file_path: str, bucket: str, s3_file_name: str,
                 prefix: str = 'raw') -> str:
    if not file_path or not os.path.isfile(file_path):
        log.error('The file was not found: %s', file_path)
        raise FileNotFoundError(f'The file was not found: {file_path}')

    key = f'{bucket}/{prefix.strip("/")}/{s3_file_name}' if prefix else f'{bucket}/{s3_file_name}'
    s3.put(file_path, key)

    local_size = os.path.getsize(file_path)
    remote_size = s3.info(key).get('size')
    if remote_size != local_size:
        raise IOError(f'Upload verification failed for {key}: local {local_size} bytes, S3 {remote_size} bytes')

    uri = f's3://{key}'
    log.info('File uploaded to %s (%d bytes)', uri, local_size)
    return uri
