"""Bound uploads before FastAPI parses multipart data."""

import asyncio
import threading
import time
from contextlib import suppress
from uuid import uuid4

from fastapi import HTTPException
from starlette.concurrency import run_in_threadpool
from starlette.responses import JSONResponse

from .service import ServiceBusy


class UploadAdmission:
    def __init__(self, settings):
        self.settings = settings
        self.active = set()
        self.waiters = set()
        self.lock = threading.Lock()

    def reserve_upload(self, wait_for_result=False):
        with self.lock:
            if len(self.active) >= self.settings.max_uploads:
                raise ServiceBusy("Upload capacity is full; retry later")
            if wait_for_result and len(self.waiters) >= self.settings.max_sync_waiters:
                raise ServiceBusy("Synchronous wait capacity is full")
            token = uuid4().hex
            if wait_for_result:
                self.waiters.add(token)
            self.active.add(token)
            return token

    def release_waiter(self, token):
        with self.lock:
            self.waiters.discard(token)

    def release_upload(self, token):
        with self.lock:
            self.active.discard(token)


class UploadGuard:
    def __init__(self, app, settings, queue_settings):
        self.app = app
        self.settings = settings
        self.config = queue_settings
        self.local = UploadAdmission(queue_settings)

    async def __call__(self, scope, receive, send):
        if (
            scope["type"] != "http"
            or scope["method"] != "POST"
            or scope["path"]
            not in {
                "/v2/parse/file",
                "/jobs",
            }
        ):
            return await self.app(scope, receive, send)
        limit = self.settings.max_upload_bytes + 1024 * 1024
        headers = dict(scope["headers"])
        try:
            length = int(headers.get(b"content-length", b"0"))
            if length < 0 or length > limit:
                return await JSONResponse({"detail": "Request body is too large"}, 413)(
                    scope, receive, send
                )
        except ValueError:
            return await JSONResponse({"detail": "Invalid Content-Length"}, 400)(
                scope, receive, send
            )
        service = scope["app"].state.service
        admission = getattr(service, "store", self.local)
        try:
            token = await run_in_threadpool(
                admission.reserve_upload, scope["path"] == "/v2/parse/file"
            )
        except ServiceBusy:
            return await JSONResponse(
                {"detail": "Upload capacity is full"}, 503, headers={"Retry-After": "5"}
            )(scope, receive, send)
        except Exception:
            return await JSONResponse({"detail": "Coordinator unavailable"}, 503)(
                scope, receive, send
            )
        state = scope.setdefault("state", {})
        state["upload_token"] = token
        state["upload_admission"] = admission
        state["upload_released"] = False
        started = time.perf_counter()
        size = 0
        finished = False

        async def guarded_receive():
            nonlocal size, finished
            if finished:
                return await receive()
            remaining = self.config.upload_seconds - (time.perf_counter() - started)
            if remaining <= 0:
                raise HTTPException(408, "Upload deadline exceeded")
            try:
                message = await asyncio.wait_for(receive(), remaining)
            except asyncio.TimeoutError as exc:
                raise HTTPException(408, "Upload deadline exceeded") from exc
            if message["type"] == "http.request":
                size += len(message.get("body", b""))
                if size > limit:
                    raise HTTPException(413, "Request body is too large")
                if not message.get("more_body", False):
                    finished = True
                    state["upload_seconds"] = time.perf_counter() - started
            return message

        async def renew():
            while not state["upload_released"]:
                await asyncio.sleep(min(10, self.config.upload_seconds / 3))
                if not state["upload_released"] and hasattr(admission, "renew_upload"):
                    try:
                        if not await run_in_threadpool(admission.renew_upload, token):
                            return
                    except Exception:
                        return

        monitor = asyncio.create_task(renew())
        try:
            await self.app(scope, guarded_receive, send)
        finally:
            monitor.cancel()
            with suppress(asyncio.CancelledError):
                await monitor
            if not state["upload_released"]:
                await run_in_threadpool(admission.release_upload, token)
                state["upload_released"] = True
            await run_in_threadpool(admission.release_waiter, token)


async def release_upload(request):
    state = request.scope["state"]
    if not state.get("upload_released", True):
        await run_in_threadpool(state["upload_admission"].release_upload, state["upload_token"])
        state["upload_released"] = True


class TransferGuard:
    """Bound result downloads and worker transfers independently of job execution."""

    def __init__(self, app, queue_settings):
        self.app = app
        self.config = queue_settings

    async def __call__(self, scope, receive, send):
        path = scope.get("path", "")
        applies = scope["type"] == "http" and (
            (scope["method"] == "GET" and path.startswith("/jobs/") and path.endswith("/result"))
            or (path.startswith("/internal/jobs/") and path.endswith(("/input", "/complete")))
        )
        if not applies:
            return await self.app(scope, receive, send)
        service = scope["app"].state.service
        store = getattr(service, "store", None)
        if store is None:
            return await self.app(scope, receive, send)
        try:
            token = await run_in_threadpool(store.reserve_transfer)
        except ServiceBusy:
            return await JSONResponse(
                {"detail": "Transfer capacity is full"}, 503, headers={"Retry-After": "1"}
            )(scope, receive, send)
        started = False

        async def tracked_send(message):
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
            await send(message)

        try:
            await asyncio.wait_for(
                self.app(scope, receive, tracked_send), self.config.transfer_seconds
            )
        except asyncio.TimeoutError:
            if started:
                raise ConnectionError("Transfer deadline exceeded") from None
            await JSONResponse({"detail": "Transfer deadline exceeded"}, 504)(scope, receive, send)
        finally:
            await run_in_threadpool(store.release_transfer, token)
