"""Bounded scheduling, request coalescing and byte-limited result caching."""

import asyncio
import hashlib
import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from cachetools import TTLCache

from .config import Settings
from .engine import MinerUEngine, ParseOutput

logger = logging.getLogger(__name__)


class ServiceBusy(Exception):
    pass


class ParseService:
    def __init__(self, settings: Settings, engine=None):
        self.settings = settings
        self.engine = engine or MinerUEngine(settings.tier)
        self.executor = ThreadPoolExecutor(
            max_workers=settings.parse_workers, thread_name_prefix="mineru"
        )
        self.cache = TTLCache(
            maxsize=settings.cache_bytes,
            ttl=settings.cache_ttl,
            getsizeof=lambda result: len(result.markdown.encode("utf-8")) + 64,
        )
        self.inflight: dict[tuple, asyncio.Task] = {}
        self.tasks: set[asyncio.Task] = set()
        self.closed = False
        self.last_error: str | None = None
        self.completed = 0
        self.ready = False

    async def warmup(self):
        await asyncio.get_running_loop().run_in_executor(self.executor, self.engine.warmup)
        self.ready = True

    async def parse(
        self, payload: bytes, encode_img: bool, disable_cache: bool, tier: str | None = None
    ):
        tier = tier or self.settings.tier
        key = (hashlib.sha256(payload).hexdigest(), encode_img, tier)
        if self.closed:
            raise ServiceBusy("Service is shutting down")
        if not disable_cache and key in self.cache:
            return self.cache[key]
        task = None if disable_cache else self.inflight.get(key)
        if task is None:
            if len(self.tasks) >= self.settings.max_pending:
                raise ServiceBusy("Parse queue is full; retry later")
            task = asyncio.create_task(self._run(payload, encode_img, key, not disable_cache, tier))
            self.tasks.add(task)
            if not disable_cache:
                self.inflight[key] = task
            task.add_done_callback(partial(self._finished, key=key))
        # A timeout/disconnect must not drop a slot while GPU work is still running.
        return await asyncio.wait_for(asyncio.shield(task), timeout=self.settings.request_timeout)

    async def _run(self, payload, encode_img, key, cache_result, tier) -> ParseOutput:
        try:
            result = await asyncio.get_running_loop().run_in_executor(
                self.executor, self.engine.parse, payload, encode_img, tier
            )
            self.last_error = None
            self.completed += 1
            self.ready = True
            if cache_result and self.cache.maxsize:
                try:
                    self.cache[key] = result
                except ValueError:
                    pass  # Oversized results are returned but not cached.
            return result
        except Exception as exc:
            self.last_error = type(exc).__name__
            logger.exception("PDF parsing failed")
            raise

    def _finished(self, task, *, key):
        self.tasks.discard(task)
        if self.inflight.get(key) is task:
            del self.inflight[key]
        if not task.cancelled():
            task.exception()  # Retrieve errors even if every caller disconnected.

    async def close(self):
        self.closed = True
        if self.tasks:
            await asyncio.gather(*list(self.tasks), return_exceptions=True)
        try:
            await asyncio.get_running_loop().run_in_executor(self.executor, self.engine.close)
        finally:
            self.executor.shutdown(wait=True)
