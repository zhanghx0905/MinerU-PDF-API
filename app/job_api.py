"""Asynchronous jobs and authenticated HTTP worker coordination."""

import json
import math
import secrets
import time
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException, Path, Request
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool
from starlette.responses import JSONResponse, Response

from .jobs import LeaseLost
from .metrics import STAGES, render_metrics
from .queue_service import QueueService

router = APIRouter()
JobID = Annotated[str, Path(pattern=r"^[a-f0-9]{32}$")]
Lease = Annotated[str, Header(alias="X-Task-Lease", pattern=r"^[a-f0-9]{32}$")]


def coordinator(request):
    service = request.app.state.service
    if not isinstance(service, QueueService):
        raise HTTPException(503, "Durable queue mode is not enabled")
    return service


def public_job(job):
    return {
        key: job[key]
        for key in (
            "id",
            "state",
            "tier",
            "encode_img",
            "pages",
            "attempts",
            "created",
            "finished",
            "error",
        )
    } | {"status_url": f"/jobs/{job['id']}", "result_url": f"/jobs/{job['id']}/result"}


@router.get("/jobs/{job_id}")
async def job_status(job_id: JobID, request: Request):
    service = coordinator(request)
    job = await run_in_threadpool(service.store.get, job_id)
    if not job:
        raise HTTPException(404, "Task not found")
    return public_job(job)


@router.get("/jobs/{job_id}/result")
async def job_result(job_id: JobID, request: Request):
    service = coordinator(request)
    job = await run_in_threadpool(service.store.get, job_id)
    if not job:
        raise HTTPException(404, "Task not found")
    if job["state"] == "failed":
        raise HTTPException(409, "Task failed")
    if job["state"] != "succeeded":
        return JSONResponse(public_job(job), 202, headers={"Retry-After": "1"})
    result = await service.result(job)
    return {
        "success": True,
        "message": "",
        "markdown": result.markdown,
        "pages": result.pages,
        "duration": job["finished"] - job["created"],
    }


@router.get("/metrics")
async def metrics(request: Request):
    service = coordinator(request)
    content = await run_in_threadpool(render_metrics, service.store)
    return Response(content, media_type="text/plain; version=0.0.4")


class Claim(BaseModel):
    worker_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    model_revision: str = Field(max_length=200)
    mineru_version: str = Field(max_length=30)


@router.post("/internal/worker/claim")
async def claim(body: Claim, request: Request):
    from importlib.metadata import version

    service = coordinator(request)
    if body.model_revision != service.config.model_revision or body.mineru_version != version(
        "mineru"
    ):
        raise HTTPException(409, "Worker model or parser version does not match coordinator")
    job = await run_in_threadpool(service.store.claim, body.worker_id)
    if job is None:
        return Response(status_code=204)
    return {
        key: job[key] for key in ("id", "lease_token", "tier", "encode_img", "pages", "attempts")
    } | {
        "encode_img": bool(job["encode_img"]),
        "lease_seconds": service.config.lease_seconds,
        "max_result_bytes": service.config.max_result_bytes,
    }


async def leased_job(request, job_id, token):
    service = coordinator(request)
    try:
        job = await run_in_threadpool(service.store.check_lease, job_id, token)
    except LeaseLost as exc:
        raise HTTPException(409, "Task lease is no longer valid") from exc
    return service, job


@router.post("/internal/jobs/{job_id}/heartbeat")
async def heartbeat(job_id: JobID, request: Request, token: Lease):
    service, _ = await leased_job(request, job_id, token)
    try:
        await run_in_threadpool(service.store.heartbeat, job_id, token)
    except LeaseLost as exc:
        raise HTTPException(409, "Task lease is no longer valid") from exc
    return {"lease_seconds": service.config.lease_seconds}


@router.get("/internal/jobs/{job_id}/input")
async def job_input(job_id: JobID, request: Request, token: Lease):
    service, job = await leased_job(request, job_id, token)
    if job["state"] != "running":
        raise HTTPException(409, "Task is already complete")
    content = await run_in_threadpool(service.storage.get, job["input_key"])
    return Response(content, media_type="application/pdf")


@router.post("/internal/jobs/{job_id}/complete")
async def complete(job_id: JobID, request: Request, token: Lease, pages: int):
    service, job = await leased_job(request, job_id, token)
    if job["state"] == "succeeded":
        return {"accepted": True}
    try:
        header = request.headers.get("X-Parse-Timings", "{}")
        if len(header) > 8192:
            raise ValueError
        timings = json.loads(header)
        if not isinstance(timings, dict) or len(timings) > len(STAGES):
            raise ValueError
        if any(
            stage not in STAGES
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
            for stage, value in timings.items()
        ):
            raise ValueError
    except (ValueError, TypeError) as exc:
        raise HTTPException(400, "Invalid stage measurements") from exc
    content = bytearray()
    async for chunk in request.stream():
        content.extend(chunk)
        if len(content) > service.config.max_result_bytes:
            raise HTTPException(413, "Result exceeds size limit")
    try:
        if not content.decode("utf-8").strip():
            raise ValueError
    except (ValueError, UnicodeDecodeError) as exc:
        raise HTTPException(400, "Result must contain UTF-8 Markdown") from exc
    key = f"results/{token}.md"
    stored_at = time.perf_counter()
    await run_in_threadpool(service.storage.put, key, bytes(content))
    timings["storage.result"] = time.perf_counter() - stored_at
    accepted = await run_in_threadpool(
        service.store.complete, job_id, token, pages, timings, len(content)
    )
    if not accepted:
        raise HTTPException(409, "Task lease or page count is no longer valid")
    return {"accepted": True}


@router.post("/internal/jobs/{job_id}/fail")
async def fail(job_id: JobID, request: Request, token: Lease):
    service, _ = await leased_job(request, job_id, token)
    try:
        await run_in_threadpool(service.store.fail, job_id, token)
    except LeaseLost as exc:
        raise HTTPException(409, "Task lease is no longer valid") from exc
    return {"accepted": True}


class WorkerAuth:
    """Reject unauthenticated worker requests before reading their bodies."""

    def __init__(self, app, token):
        self.app = app
        self.token = token

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["path"].startswith("/internal/"):
            supplied = dict(scope["headers"]).get(b"authorization", b"")
            expected = ("Bearer " + self.token).encode()
            if not self.token or not secrets.compare_digest(supplied, expected):
                return await JSONResponse({"detail": "Worker authentication required"}, 401)(
                    scope, receive, send
                )
        await self.app(scope, receive, send)
