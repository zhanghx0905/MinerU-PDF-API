"""Compatibility API: POST /v2/parse/file, plus service and model status."""

import asyncio
import io
import os
import time
from contextlib import asynccontextmanager, suppress
from importlib.metadata import PackageNotFoundError, version
from typing import Annotated, Literal

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pypdf import PdfReader
from starlette.concurrency import run_in_threadpool

from .admission import TransferGuard, UploadGuard, release_upload
from .config import Settings
from .job_api import WorkerAuth, public_job, router
from .jobs import IdempotencyConflict, WorkBudgetExceeded
from .queue_config import QueueSettings
from .queue_service import QueueService
from .service import ParseService, ServiceBusy


class ParseResponse(BaseModel):
    success: bool
    message: str = ""
    markdown: str = ""
    pages: int = 0
    duration: float


def validate_pdf(payload: bytes, max_pages: int):
    if b"%PDF-" not in payload[:1024]:
        raise HTTPException(400, "Invalid PDF signature")
    try:
        reader = PdfReader(io.BytesIO(payload))
        if reader.is_encrypted and not reader.decrypt(""):
            raise HTTPException(400, "Password-protected PDFs are not supported")
        pages = len(reader.pages)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(400, "Invalid or damaged PDF") from exc
    if not pages:
        raise HTTPException(400, "PDF has no pages")
    if pages > max_pages:
        raise HTTPException(413, f"PDF exceeds {max_pages} pages")
    return pages


def create_app(settings: Settings | None = None, *, engine=None, queue_settings=None):
    settings = settings or Settings.from_env()
    queue_settings = queue_settings or QueueSettings.from_env()

    @asynccontextmanager
    async def lifespan(app):
        durable = settings.service_mode == "queue"
        app.state.service = (
            QueueService(settings, queue_settings) if durable else ParseService(settings, engine)
        )
        maintenance = None
        try:
            if durable:
                maintenance = asyncio.create_task(app.state.service.maintenance())
            elif settings.preload_models and engine is None:
                await app.state.service.warmup()
            yield
        finally:
            await app.state.service.close()
            if maintenance:
                maintenance.cancel()
                with suppress(asyncio.CancelledError):
                    await maintenance

    app = FastAPI(
        title="MinerU PDF Service", version=version("mineru-pdf-service"), lifespan=lifespan
    )

    app.add_middleware(UploadGuard, settings=settings, queue_settings=queue_settings)
    app.add_middleware(TransferGuard, queue_settings=queue_settings)
    app.add_middleware(WorkerAuth, token=queue_settings.worker_token)
    app.include_router(router)

    @app.get("/health")
    async def health(request: Request):
        service = request.app.state.service
        try:
            mineru_version = version("mineru")
        except PackageNotFoundError:
            mineru_version = "not installed"
        if isinstance(service, QueueService):
            stats = await run_in_threadpool(service.store.stats)
            return {
                "status": "ok",
                "mode": "queue",
                "service_version": version("mineru-pdf-service"),
                "mineru_version": mineru_version,
                "tier": settings.tier,
                "model_ready": stats["workers"] > 0,
                "last_error": service.last_error,
                "pending": stats["queued"] + stats["running"],
                "completed": stats["succeeded"],
                **stats,
            }
        return {
            "status": "stopping" if service.closed else "ok",
            "mineru_version": mineru_version,
            "tier": settings.tier,
            "pending": len(service.tasks),
            "completed": service.completed,
            "last_error": service.last_error,
            "model_ready": service.ready,
            "parse_workers": settings.parse_workers,
            "small_backend": os.getenv("MINERU_MODEL_SMALL_BACKEND", "auto"),
        }

    @app.post("/v2/parse/file", response_model=ParseResponse)
    async def parse(
        request: Request,
        file: Annotated[UploadFile, File()],
        disable_cache: bool = False,
        encode_img: bool = True,
        tier: Literal["flash", "basic", "standard", "advanced"] | None = None,
    ):
        started = time.perf_counter()
        try:
            if not file.filename or not file.filename.lower().endswith(".pdf"):
                raise HTTPException(400, "Unsupported file type; upload a PDF")
            payload = await file.read(settings.max_upload_bytes + 1)
            if len(payload) > settings.max_upload_bytes:
                raise HTTPException(413, "Uploaded PDF is too large")
            validated = time.perf_counter()
            pages = await run_in_threadpool(validate_pdf, payload, settings.max_pages)
            validation_seconds = time.perf_counter() - validated
            try:
                service = request.app.state.service
                if isinstance(service, QueueService):
                    job, _ = await service.submit(
                        payload,
                        pages,
                        encode_img,
                        disable_cache,
                        tier,
                        upload_token=request.state.upload_token,
                        timings={
                            "api.validate": validation_seconds,
                            "api.upload": getattr(request.state, "upload_seconds", 0),
                        },
                    )
                    del payload
                    await file.close()
                    await release_upload(request)
                    result = await service.wait(job["id"])
                else:
                    await file.close()
                    await release_upload(request)
                    result = await service.parse(payload, encode_img, disable_cache, tier)
            except ServiceBusy as exc:
                raise HTTPException(503, str(exc), headers={"Retry-After": "5"}) from exc
            except WorkBudgetExceeded as exc:
                raise HTTPException(413, str(exc)) from exc
            except asyncio.TimeoutError:
                return failure(504, "Timed out waiting for PDF parsing", started)
            except Exception:
                return failure(500, "PDF parsing failed; see server logs", started)
            return ParseResponse(
                success=True,
                markdown=result.markdown,
                pages=result.pages,
                duration=time.perf_counter() - started,
            )
        finally:
            await file.close()

    @app.post("/jobs", status_code=202)
    async def submit_job(
        request: Request,
        file: Annotated[UploadFile, File()],
        disable_cache: bool = False,
        encode_img: bool = True,
        tier: Literal["flash", "basic", "standard", "advanced"] | None = None,
    ):
        service = request.app.state.service
        try:
            if not isinstance(service, QueueService):
                raise HTTPException(503, "Durable queue mode is not enabled")
            if not file.filename or not file.filename.lower().endswith(".pdf"):
                raise HTTPException(400, "Unsupported file type; upload a PDF")
            payload = await file.read(settings.max_upload_bytes + 1)
            if len(payload) > settings.max_upload_bytes:
                raise HTTPException(413, "Uploaded PDF is too large")
            started = time.perf_counter()
            pages = await run_in_threadpool(validate_pdf, payload, settings.max_pages)
            validation_seconds = time.perf_counter() - started
            key = request.headers.get("Idempotency-Key")
            if key is not None and (not key.strip() or len(key) > 128):
                raise HTTPException(400, "Idempotency-Key must contain 1 to 128 characters")
            try:
                job, reused = await service.submit(
                    payload,
                    pages,
                    encode_img,
                    disable_cache,
                    tier,
                    upload_token=request.state.upload_token,
                    idempotency_key=key,
                    timings={
                        "api.validate": validation_seconds,
                        "api.upload": getattr(request.state, "upload_seconds", 0),
                    },
                )
            except ServiceBusy as exc:
                raise HTTPException(503, str(exc), headers={"Retry-After": "5"}) from exc
            except IdempotencyConflict as exc:
                raise HTTPException(409, str(exc)) from exc
            except WorkBudgetExceeded as exc:
                raise HTTPException(413, "Document exceeds the queue work budget") from exc
            return JSONResponse(
                public_job(job) | {"reused": reused},
                202,
                headers={"Location": f"/jobs/{job['id']}"},
            )
        finally:
            await file.close()
            await release_upload(request)

    return app


def failure(status: int, message: str, started: float):
    return JSONResponse(
        status_code=status,
        content=ParseResponse(
            success=False, message=message, duration=time.perf_counter() - started
        ).model_dump(),
    )


app = create_app()
