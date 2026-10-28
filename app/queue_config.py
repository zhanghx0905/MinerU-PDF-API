"""Portable durable-queue configuration, shared by API and workers."""

import math
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class QueueSettings:
    database: str = "data/queue.sqlite3"
    object_dir: str = "data/objects"
    storage: str = "local"
    s3_bucket: str = ""
    s3_endpoint: str | None = None
    worker_token: str = ""
    worker_api_url: str = "http://127.0.0.1:3888"
    max_work: int = 4096
    max_uploads: int = 8
    max_sync_waiters: int = 8
    max_transfers: int = 8
    transfer_seconds: float = 120
    upload_seconds: float = 120
    lease_seconds: float = 60
    heartbeat_seconds: float = 10
    poll_seconds: float = 0.25
    max_attempts: int = 3
    max_queue_seconds: float = 300
    retention_seconds: float = 86400
    max_result_bytes: int = 64 * 1024 * 1024
    model_revision: str = "MinerU2.5-Pro-2605-1.2B"

    def __post_init__(self):
        for name in (
            "max_work",
            "max_uploads",
            "max_sync_waiters",
            "max_transfers",
            "upload_seconds",
            "transfer_seconds",
            "lease_seconds",
            "heartbeat_seconds",
            "poll_seconds",
            "max_attempts",
            "max_queue_seconds",
            "retention_seconds",
            "max_result_bytes",
        ):
            if not math.isfinite(getattr(self, name)) or getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.heartbeat_seconds >= self.lease_seconds / 2:
            raise ValueError("Heartbeat interval must be less than half of the lease")
        if self.storage not in {"local", "s3"}:
            raise ValueError("OBJECT_STORAGE must be local or s3")
        if self.storage == "s3" and not self.s3_bucket:
            raise ValueError("S3_BUCKET is required for S3 storage")

    @classmethod
    def from_env(cls):
        defaults = cls()
        values = {}
        names = {
            "database": "QUEUE_DATABASE",
            "object_dir": "OBJECT_DIR",
            "storage": "OBJECT_STORAGE",
            "s3_bucket": "S3_BUCKET",
            "s3_endpoint": "S3_ENDPOINT_URL",
            "worker_token": "WORKER_API_TOKEN",
            "worker_api_url": "WORKER_API_URL",
            "model_revision": "MODEL_REVISION",
        }
        for name, env in names.items():
            values[name] = os.getenv(env, getattr(defaults, name))
        for name in (
            "max_work",
            "max_uploads",
            "max_sync_waiters",
            "max_transfers",
            "max_attempts",
            "max_result_bytes",
        ):
            values[name] = int(os.getenv("QUEUE_" + name.upper(), getattr(defaults, name)))
        for name in (
            "upload_seconds",
            "transfer_seconds",
            "lease_seconds",
            "heartbeat_seconds",
            "poll_seconds",
            "max_queue_seconds",
            "retention_seconds",
        ):
            values[name] = float(os.getenv("QUEUE_" + name.upper(), getattr(defaults, name)))
        values["s3_endpoint"] = values["s3_endpoint"] or None
        return cls(**values)
