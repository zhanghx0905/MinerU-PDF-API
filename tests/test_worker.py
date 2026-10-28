import asyncio
import json
from importlib.metadata import version
from uuid import uuid4

import httpx
from fastapi.testclient import TestClient

from app.config import Settings
from app.engine import ParseOutput
from app.main import create_app
from app.queue_config import QueueSettings
from app.vlm_gateway import create_gateway
from app.worker import Worker
from tests.test_queue import pdf


class StubEngine:
    def parse(self, _payload, encode_img, tier):
        return ParseOutput(f"{tier}:{encode_img}", 1, {"sdk.total": 0.1})

    def close(self):
        pass


def test_worker_executes_through_real_http_protocol(tmp_path):
    settings = Settings(service_mode="queue", preload_models=False)
    config = QueueSettings(
        database=str(tmp_path / "queue.db"),
        object_dir=str(tmp_path / "objects"),
        worker_token="test-worker",
    )
    with TestClient(create_app(settings, queue_settings=config)) as client:
        submitted = client.post(
            "/jobs?tier=flash&encode_img=false", files={"file": ("sample.pdf", pdf())}
        ).json()
        client.headers["Authorization"] = "Bearer " + config.worker_token
        worker = Worker(
            settings,
            config,
            engine=StubEngine(),
            client=client,
            fatal_exit=lambda code: (_ for _ in ()).throw(AssertionError(code)),
        )
        worker.execute(worker.claim())
        result = client.get(submitted["result_url"])
        assert result.status_code == 200
        assert result.json()["markdown"] == "flash:False"
        assert 'stage="sdk.total"' in client.get("/metrics").text


def test_invalid_measurements_and_wrong_version_are_rejected(tmp_path):
    config = QueueSettings(
        database=str(tmp_path / "q.db"), object_dir=str(tmp_path / "o"), worker_token="test-worker"
    )
    with TestClient(create_app(Settings(service_mode="queue"), queue_settings=config)) as client:
        client.headers["Authorization"] = "Bearer " + config.worker_token
        assert (
            client.post(
                "/internal/worker/claim",
                json={
                    "worker_id": uuid4().hex,
                    "model_revision": config.model_revision,
                    "mineru_version": "wrong",
                },
            ).status_code
            == 409
        )
        client.post("/jobs", files={"file": ("test.pdf", pdf())})
        claimed = client.post(
            "/internal/worker/claim",
            json={
                "worker_id": uuid4().hex,
                "model_revision": config.model_revision,
                "mineru_version": version("mineru"),
            },
        ).json()
        headers = {
            "X-Task-Lease": claimed["lease_token"],
            "X-Parse-Timings": json.dumps({"private-label": 1}),
        }
        response = client.post(
            f"/internal/jobs/{claimed['id']}/complete?pages=1", headers=headers, content=b"markdown"
        )
        assert response.status_code == 400


def test_gateway_balances_concurrent_requests_and_releases_slots():
    async def scenario():
        seen = []
        both = asyncio.Event()

        async def upstream(request):
            seen.append(request.url.host)
            if len(seen) >= 2:
                both.set()
            await asyncio.wait_for(both.wait(), 2)
            return httpx.Response(
                200,
                stream=httpx.ByteStream(b'{"ok": true}'),
                headers={"content-type": "application/json"},
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as upstream_client:
            app = create_gateway(
                ["http://replica-a", "http://replica-b"], upstream_client, capacity=1
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                responses = await asyncio.gather(
                    *[client.post("/v1/chat/completions", json={"messages": []}) for _ in range(2)]
                )
                assert all(response.status_code == 200 for response in responses)
                assert set(seen) == {"replica-a", "replica-b"}
                assert app.state.router.active == [0, 0]
                metrics = (await client.get("/metrics")).text
                assert "mineru_vlm_http_seconds_count" in metrics
                assert "replica-a" not in metrics

    asyncio.run(scenario())


def test_gateway_failure_does_not_leak_admission_slot():
    async def scenario():
        async def failed(request):
            raise httpx.ConnectError("private upstream detail", request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(failed)) as upstream_client:
            app = create_gateway(["http://replica"], upstream_client, capacity=1)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.post("/v1/chat/completions", json={})
                assert response.status_code == 502
                assert "private" not in response.text
                assert app.state.router.active == [0]

    asyncio.run(scenario())


def test_sdk_stage_capture_uses_installed_sdk_timer(monkeypatch):
    import time

    from mineru.utils.timing import stage_timer

    from app.metrics import record_sdk_stages

    monkeypatch.setenv("MINERU_PROFILE_STAGES", "true")
    timings = {}
    with record_sdk_stages(timings), stage_timer("pdf.layout"):
        time.sleep(0.001)
    assert timings["pdf.layout"] > 0


def test_worker_stops_stale_gpu_execution():
    import threading

    stopped = threading.Event()
    exits = []

    class BlockingEngine:
        def parse(self, *_args):
            assert stopped.wait(2)
            return ParseOutput("stale", 1)

    class LostClient:
        def get(self, *_args, **_kwargs):
            return httpx.Response(200, content=b"pdf", request=httpx.Request("GET", "http://test"))

        def post(self, path, **_kwargs):
            assert path.endswith("/heartbeat"), "A stale worker attempted to publish a result"
            return httpx.Response(409, request=httpx.Request("POST", "http://test"))

    def terminate(code):
        exits.append(code)
        stopped.set()

    config = QueueSettings(worker_token="test-worker", lease_seconds=0.5, heartbeat_seconds=0.02)
    worker = Worker(
        Settings(), config, engine=BlockingEngine(), client=LostClient(), fatal_exit=terminate
    )
    worker.execute(
        {
            "id": uuid4().hex,
            "lease_token": uuid4().hex,
            "lease_seconds": 0.5,
            "tier": "standard",
            "encode_img": True,
            "max_result_bytes": 100,
        }
    )
    assert exits == [70]
