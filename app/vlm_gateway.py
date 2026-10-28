"""Bounded least-active routing and timing for independent vLLM replicas."""

import asyncio
import os
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request
from prometheus_client import CollectorRegistry, Gauge, Histogram, generate_latest
from starlette.background import BackgroundTask
from starlette.responses import Response, StreamingResponse


class Router:
    def __init__(self, upstreams, client, capacity=64, max_waiting=128, queue_timeout=60):
        if not upstreams or min(capacity, max_waiting, queue_timeout) <= 0:
            raise ValueError("Invalid VLM routing limits")
        self.upstreams = upstreams
        self.client = client
        self.capacity = capacity
        self.max_waiting = max_waiting
        self.queue_timeout = queue_timeout
        self.active = [0] * len(upstreams)
        self.cooldown = [0.0] * len(upstreams)
        self.waiting = 0
        self.condition = asyncio.Condition()
        self.registry = CollectorRegistry()
        self.duration = Histogram(
            "mineru_vlm_http_seconds",
            "Upstream wall time including response",
            ["replica"],
            registry=self.registry,
            buckets=(0.01, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 300),
        )
        self.queue_time = Histogram(
            "mineru_vlm_queue_seconds", "Gateway admission wait", registry=self.registry
        )
        self.gauge = Gauge(
            "mineru_vlm_active", "Active requests", ["replica"], registry=self.registry
        )

    async def acquire(self):
        started = time.perf_counter()
        async with self.condition:
            if self.waiting >= self.max_waiting:
                raise HTTPException(
                    503, "VLM admission queue is full", headers={"Retry-After": "1"}
                )
            self.waiting += 1
            try:
                while True:
                    now = time.monotonic()
                    candidates = [
                        i
                        for i, active in enumerate(self.active)
                        if active < self.capacity and self.cooldown[i] <= now
                    ]
                    if candidates:
                        index = min(candidates, key=lambda i: (self.active[i], i))
                        self.active[index] += 1
                        self.gauge.labels(str(index)).inc()
                        self.queue_time.observe(time.perf_counter() - started)
                        return index
                    if time.perf_counter() - started >= self.queue_timeout:
                        raise HTTPException(503, "VLM admission deadline exceeded")
                    try:
                        await asyncio.wait_for(self.condition.wait(), 0.25)
                    except asyncio.TimeoutError:
                        pass
            finally:
                self.waiting -= 1

    async def release(self, index):
        async with self.condition:
            self.active[index] -= 1
            self.gauge.labels(str(index)).dec()
            self.condition.notify_all()


def create_gateway(upstreams=None, client=None, **limits):
    upstreams = upstreams or os.getenv("VLM_UPSTREAMS", "http://mineru-vlm:8000").split(",")
    upstreams = [url.strip().rstrip("/") for url in upstreams if url.strip()]
    if any(not url.startswith(("http://", "https://")) for url in upstreams):
        raise ValueError("VLM upstreams must use HTTP or HTTPS")
    owned = client is None
    client = client or httpx.AsyncClient(
        timeout=httpx.Timeout(600, connect=5),
        trust_env=False,
        limits=httpx.Limits(max_connections=256),
    )
    limits.setdefault("capacity", int(os.getenv("VLM_MAX_ACTIVE_PER_REPLICA", "64")))
    limits.setdefault("max_waiting", int(os.getenv("VLM_MAX_WAITING", "128")))
    limits.setdefault("queue_timeout", float(os.getenv("VLM_QUEUE_SECONDS", "60")))
    router = Router(upstreams, client, **limits)

    @asynccontextmanager
    async def lifespan(_app):
        yield
        if owned:
            await client.aclose()

    app = FastAPI(lifespan=lifespan)
    app.state.router = router

    @app.get("/health")
    async def health():
        return {"status": "ok", "replicas": len(upstreams), "active": sum(router.active)}

    @app.get("/metrics")
    async def metrics():
        return Response(generate_latest(router.registry), media_type="text/plain; version=0.0.4")

    @app.api_route("/v1/{path:path}", methods=["GET", "POST"])
    async def forward(path, request: Request):
        if (request.method, path) not in {("GET", "models"), ("POST", "chat/completions")}:
            raise HTTPException(404, "Unsupported inference route")
        index = await router.acquire()
        started = time.perf_counter()
        released = False
        response = None

        async def finish():
            nonlocal released
            if not released:
                released = True
                if response is not None:
                    await response.aclose()
                router.duration.labels(str(index)).observe(time.perf_counter() - started)
                await router.release(index)

        async def content():
            total = 0
            async for chunk in request.stream():
                total += len(chunk)
                if total > 128 * 1024 * 1024:
                    raise HTTPException(413, "Inference input exceeds limit")
                yield chunk

        try:
            upstream_request = client.build_request(
                request.method,
                upstreams[index] + "/v1/" + path,
                headers={
                    name: value
                    for name, value in request.headers.items()
                    if name.lower() in {"content-type", "authorization"}
                },
                content=content(),
            )
            response = await client.send(upstream_request, stream=True)
            if response.status_code >= 500:
                router.cooldown[index] = time.monotonic() + 5
        except BaseException as exc:
            router.cooldown[index] = time.monotonic() + 5
            await finish()
            if isinstance(exc, httpx.HTTPError):
                raise HTTPException(502, "Inference upstream unavailable") from exc
            raise

        async def stream():
            try:
                async for chunk in response.aiter_raw():
                    yield chunk
            finally:
                await asyncio.shield(finish())

        return StreamingResponse(
            stream(),
            status_code=response.status_code,
            headers={
                key: value
                for key, value in response.headers.items()
                if key.lower() in {"content-type", "content-encoding"}
            },
            background=BackgroundTask(finish),
        )

    return app


app = create_gateway()
