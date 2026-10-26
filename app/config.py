"""Validated service configuration. Engine options use MinerU's own env vars."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    tier: str = "standard"
    preload_models: bool = True
    parse_workers: int = 2
    max_upload_bytes: int = 50 * 1024 * 1024
    max_pages: int = 1000
    max_pending: int = 8
    request_timeout: float = 6000
    cache_bytes: int = 128 * 1024 * 1024
    cache_ttl: float = 3600

    def __post_init__(self):
        if self.tier not in {"flash", "basic", "standard", "advanced"}:
            raise ValueError("PARSE_TIER must be flash, basic, standard or advanced")
        for name in (
            "max_upload_bytes",
            "max_pages",
            "max_pending",
            "parse_workers",
            "request_timeout",
            "cache_ttl",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.cache_bytes < 0:
            raise ValueError("cache_bytes must be nonnegative")

    @classmethod
    def from_env(cls):
        defaults = cls()
        return cls(
            tier=os.getenv("PARSE_TIER", defaults.tier),
            preload_models=os.getenv("PRELOAD_MODELS", "true").lower() in {"1", "true", "yes"},
            parse_workers=int(os.getenv("PARSE_WORKERS", defaults.parse_workers)),
            max_upload_bytes=int(os.getenv("MAX_UPLOAD_BYTES", defaults.max_upload_bytes)),
            max_pages=int(os.getenv("MAX_PAGES", defaults.max_pages)),
            max_pending=int(os.getenv("MAX_PENDING", defaults.max_pending)),
            request_timeout=float(os.getenv("REQUEST_TIMEOUT", defaults.request_timeout)),
            cache_bytes=int(os.getenv("CACHE_BYTES", defaults.cache_bytes)),
            cache_ttl=float(os.getenv("CACHE_TTL", defaults.cache_ttl)),
        )
