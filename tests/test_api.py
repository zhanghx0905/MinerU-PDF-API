import asyncio
import io
import threading

import httpx
import pytest
from fastapi.testclient import TestClient
from pypdf import PdfWriter

from app.config import Settings
from app.engine import ParseOutput
from app.main import create_app
from app.service import ParseService, ServiceBusy


def make_pdf(pages=1):
    writer = PdfWriter()
    for _ in range(pages):
        writer.add_blank_page(width=600, height=800)
    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


class FakeEngine:
    def __init__(self):
        self.calls = 0
        self.closed = False

    def parse(self, payload, encode_img, tier=None):
        self.calls += 1
        return ParseOutput(
            "# Result" + (" ![](data:image/png;base64,YQ==)" if encode_img else ""), 1
        )

    def close(self):
        self.closed = True


def test_contract_cache_and_encoding():
    engine = FakeEngine()
    with TestClient(create_app(Settings(), engine=engine)) as client:
        upload = {"file": ("test.PDF", make_pdf(), "application/pdf")}
        first = client.post("/v2/parse/file", files=upload)
        assert first.status_code == 200
        assert first.json()["success"] is True
        assert first.json()["pages"] == 1
        assert "data:image" in first.json()["markdown"]
        assert client.post("/v2/parse/file", files=upload).status_code == 200
        assert engine.calls == 1
        plain = client.post("/v2/parse/file?encode_img=false", files=upload)
        assert plain.json()["markdown"] == "# Result"
        assert engine.calls == 2
        client.post("/v2/parse/file?disable_cache=true", files=upload)
        assert engine.calls == 3
        client.post("/v2/parse/file?tier=flash", files=upload)
        assert engine.calls == 4
        client.post("/v2/parse/file?tier=flash", files=upload)
        assert engine.calls == 4
        assert client.post("/v2/parse/file?tier=unknown", files=upload).status_code == 422
        health = client.get("/health").json()
        assert health["status"] == "ok"
        assert health["tier"] == "standard"
    assert engine.closed


@pytest.mark.parametrize(
    "name,payload,status",
    [
        ("x.txt", b"hi", 400),
        ("x.pdf", b"not a pdf", 400),
        ("empty.pdf", b"", 400),
        ("broken.pdf", b"%PDF-1.7\nbroken", 400),
        ("large.pdf", b"a" * 2001, 413),
        ("pages.pdf", make_pdf(2), 413),
    ],
)
def test_rejects_invalid_files(name, payload, status):
    engine = FakeEngine()
    with TestClient(create_app(Settings(max_upload_bytes=2000, max_pages=1), engine=engine)) as c:
        response = c.post("/v2/parse/file", files={"file": (name, payload)})
    assert response.status_code == status
    assert engine.calls == 0


def test_encrypted_pdf():
    writer = PdfWriter()
    writer.add_blank_page(600, 800)
    writer.encrypt("secret")
    payload = io.BytesIO()
    writer.write(payload)
    with TestClient(create_app(engine=FakeEngine())) as c:
        r = c.post("/v2/parse/file", files={"file": ("locked.pdf", payload.getvalue())})
        assert r.status_code == 400


def test_failure_returns_500_and_next_request_recovers():
    class FailingEngine(FakeEngine):
        def parse(self, payload, encode_img, tier=None):
            if not self.calls:
                self.calls += 1
                raise RuntimeError("private path /secret/file")
            return super().parse(payload, encode_img, tier)

    with TestClient(create_app(engine=FailingEngine())) as c:
        upload = {"file": ("test.pdf", make_pdf())}
        failed = c.post("/v2/parse/file", files=upload)
        assert failed.status_code == 500
        assert failed.json()["success"] is False
        assert "secret" not in failed.text
        assert c.post("/v2/parse/file", files=upload).json()["success"]


def test_timeout_keeps_queue_slot_and_coalesces_work():
    class BlockingEngine(FakeEngine):
        def __init__(self):
            super().__init__()
            self.release = threading.Event()

        def parse(self, payload, encode_img, tier=None):
            self.release.wait(5)
            return super().parse(payload, encode_img, tier)

    async def scenario():
        engine = BlockingEngine()
        service = ParseService(Settings(max_pending=1, request_timeout=0.02), engine)
        try:
            with pytest.raises(asyncio.TimeoutError):
                await service.parse(b"pdf", True, False)
            assert len(service.tasks) == 1
            with pytest.raises(ServiceBusy):
                await service.parse(b"other pdf", True, False)
            with pytest.raises(asyncio.TimeoutError):
                await service.parse(b"pdf", True, False)
            engine.release.set()
            await asyncio.gather(*list(service.tasks))
            assert (await service.parse(b"pdf", True, False)).pages == 1
            assert engine.calls == 1
        finally:
            engine.release.set()
            await service.close()

    asyncio.run(scenario())


def test_concurrent_uploads_share_one_parse():
    async def scenario():
        engine = FakeEngine()
        app = create_app(engine=engine)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                upload = {"file": ("test.pdf", make_pdf())}
                results = await asyncio.gather(
                    *[client.post("/v2/parse/file", files=upload) for _ in range(5)]
                )
                assert all(r.status_code == 200 for r in results)
                assert engine.calls == 1

    asyncio.run(scenario())


def test_oversized_cache_result_still_returns():
    async def scenario():
        engine = FakeEngine()
        service = ParseService(Settings(cache_bytes=1), engine)
        try:
            await service.parse(b"a", False, False)
            await service.parse(b"a", False, False)
            assert engine.calls == 2
        finally:
            await service.close()

    asyncio.run(scenario())


def test_parallel_requests_keep_results_separate():
    class ParallelEngine(FakeEngine):
        barrier = threading.Barrier(2)

        def parse(self, payload, encode_img, tier=None):
            self.barrier.wait(timeout=3)
            return ParseOutput(payload.decode(), 1)

    async def scenario():
        service = ParseService(Settings(parse_workers=2), ParallelEngine())
        try:
            first, second = await asyncio.gather(
                service.parse(b"first", True, True),
                service.parse(b"second", False, True),
            )
            assert first.markdown == "first"
            assert second.markdown == "second"
        finally:
            await service.close()

    asyncio.run(scenario())
