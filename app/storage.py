"""Immutable object storage; object keys never contain supplied file names."""

import os
import re
from pathlib import Path
from tempfile import NamedTemporaryFile

from .queue_config import QueueSettings

_KEY = re.compile(r"(?:inputs|results)/[a-f0-9]{32}\.(?:pdf|md)\Z")


def validate_key(key):
    if not _KEY.fullmatch(key):
        raise ValueError("Invalid object key")


class LocalStorage:
    def __init__(self, directory):
        self.root = Path(directory)
        self.root.mkdir(parents=True, exist_ok=True)

    def put(self, key, data):
        validate_key(key)
        target = self.root / key
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        try:
            with NamedTemporaryFile(dir=target.parent, delete=False) as output:
                temporary = Path(output.name)
                output.write(data)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, target)
            descriptor = os.open(target.parent, os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        finally:
            if temporary:
                temporary.unlink(missing_ok=True)

    def get(self, key):
        validate_key(key)
        return (self.root / key).read_bytes()

    def delete(self, key):
        validate_key(key)
        (self.root / key).unlink(missing_ok=True)


class S3Storage:
    def __init__(self, bucket, endpoint=None):
        import boto3
        from botocore.config import Config

        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            config=Config(
                connect_timeout=5, read_timeout=30, retries={"max_attempts": 2, "mode": "standard"}
            ),
        )
        self.bucket = bucket

    def put(self, key, data):
        validate_key(key)
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)

    def get(self, key):
        validate_key(key)
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        try:
            return response["Body"].read()
        finally:
            response["Body"].close()

    def delete(self, key):
        validate_key(key)
        self.client.delete_object(Bucket=self.bucket, Key=key)


def create_storage(settings: QueueSettings):
    if settings.storage == "s3":
        return S3Storage(settings.s3_bucket, settings.s3_endpoint)
    return LocalStorage(settings.object_dir)
