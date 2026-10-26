"""Compatibility API: POST /v2/parse/file, plus service and model status."""

import asyncio
import io
import os
import time
from contextlib import asynccontextmanager
from importlib.metadata import PackageNotFoundError, version
from typing import Annotated, Literal

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pypdf import PdfReader
from starlette.concurrency import run_in_threadpool

from .config import Settings
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


def create_app(settings: Settings | None = None, *, engine=None):
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app):
        app.state.service = ParseService(settings, engine)
        try:
            if settings.preload_models and engine is None:
                await app.state.service.warmup()
            yield
        finally:
            await app.state.service.close()

    app = FastAPI(title="MinerU PDF Service", version="1.0.0", lifespan=lifespan)

    @app.get("/health")
    async def health(request: Request):
        service = request.app.state.service
        try:
            mineru_version = version("mineru")
        except PackageNotFoundError:
            mineru_version = "not installed"
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
            await run_in_threadpool(validate_pdf, payload, settings.max_pages)
            try:
                result = await request.app.state.service.parse(
                    payload, encode_img, disable_cache, tier
                )
            except ServiceBusy as exc:
                raise HTTPException(503, str(exc), headers={"Retry-After": "5"}) from exc
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

    return app


def failure(status: int, message: str, started: float):
    return JSONResponse(
        status_code=status,
        content=ParseResponse(
            success=False, message=message, duration=time.perf_counter() - started
        ).model_dump(),
    )


app = create_app()
