"""Real SQLite transactions, HTTP coordination, and durable recovery."""

import asyncio
import io
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import replace
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from pypdf import PdfWriter

from app.config import Settings
from app.jobs import IdempotencyConflict, LeaseLost, QueueStore
from app.main import create_app
from app.queue_config import QueueSettings
from app.service import ServiceBusy


def pdf(pages=1):
    writer = PdfWriter()
    for _ in range(pages):
        writer.add_blank_page(width=100, height=100)
    result = io.BytesIO()
    writer.write(result)
    return result.getvalue()


@pytest.fixture
def configuration(tmp_path):
    return Settings(service_mode="queue", preload_models=False, max_pending=8), QueueSettings(
        database=str(tmp_path / "jobs.db"),
        object_dir=str(tmp_path / "objects"),
        worker_token="test-worker-token",
        poll_seconds=0.01,
    )


def submit(store, fingerprint=None, pages=1, **kwargs):
    return store.submit(
        fingerprint=fingerprint or uuid4().hex,
        input_key=f"inputs/{uuid4().hex}.pdf",
        pages=pages,
        tier="standard",
        encode_img=True,
        **kwargs,
    )


def claim_in_process(settings, config):
    return QueueStore(settings, config).claim(uuid4().hex)


def test_concurrent_processes_claim_distinct_jobs_and_share_budget(configuration):
    settings, config = configuration
    store = QueueStore(settings, config)
    for _ in range(8):
        submit(store)
    with pytest.raises(ServiceBusy):
        submit(QueueStore(settings, config))
    with ProcessPoolExecutor(max_workers=4) as pool:
        claimed = list(pool.map(claim_in_process, [settings] * 12, [config] * 12))
    assert len({job["id"] for job in claimed if job}) == 8
    assert store.stats()["running"] == 8


def test_global_deduplication_and_explicit_idempotency(configuration):
    settings, config = configuration
    stores = [QueueStore(settings, config), QueueStore(settings, config)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        jobs = list(pool.map(lambda i: submit(stores[i % 2], "identical"), range(16)))
    assert len({job["id"] for job, _ in jobs}) == 1
    one, _ = submit(stores[0], "one", idempotency_key="retry-key", disable_cache=True)
    same, reused = submit(stores[1], "one", idempotency_key="retry-key", disable_cache=True)
    assert reused and one["id"] == same["id"]
    with pytest.raises(IdempotencyConflict):
        submit(stores[1], "different", idempotency_key="retry-key")
    a, _ = submit(stores[0], "bypass", disable_cache=True)
    b, _ = submit(stores[0], "bypass", disable_cache=True)
    assert a["id"] != b["id"]


def test_expired_lease_is_retried_and_stale_worker_cannot_commit(configuration):
    settings, config = configuration
    now = [100.0]
    store = QueueStore(settings, replace(config, max_attempts=2), clock=lambda: now[0])
    original, _ = submit(store)
    first = store.claim(uuid4().hex)
    now[0] += config.lease_seconds + 1
    store.maintenance()
    assert store.get(original["id"])["state"] == "queued"
    assert not store.complete(first["id"], first["lease_token"], 1, {})
    with pytest.raises(LeaseLost):
        store.heartbeat(first["id"], first["lease_token"])
    now[0] += 3
    second = store.claim(uuid4().hex)
    assert second["attempts"] == 2 and second["lease_token"] != first["lease_token"]
    assert store.complete(second["id"], second["lease_token"], 1, {"sdk.total": 1.2})
    assert store.complete(second["id"], second["lease_token"], 1, {"sdk.total": 1.2})
    rows, _ = store.metric_rows()
    assert next(row for row in rows if row["stage"] == "sdk.total")["count"] == 1
    assert store.get(original["id"])["state"] == "succeeded"
    reopened = QueueStore(settings, config, clock=lambda: now[0])
    assert reopened.get(original["id"])["state"] == "succeeded"


def test_queue_deadline_retry_exhaustion_and_cleanup(configuration):
    settings, config = configuration
    now = [100.0]
    store = QueueStore(settings, replace(config, max_attempts=1), clock=lambda: now[0])
    job, _ = submit(store)
    attempt = store.claim(uuid4().hex)
    store.fail(job["id"], attempt["lease_token"])
    assert store.get(job["id"])["state"] == "failed"
    waiting, _ = submit(store)
    now[0] += config.max_queue_seconds + 1
    store.maintenance()
    assert store.get(waiting["id"])["error"] == "queue_timeout"
    now[0] += config.retention_seconds + 1
    store.maintenance()
    assert store.get(job["id"]) is None
    assert store.stats()["work_units"] == 0
    assert store.garbage()


def test_work_budget_and_short_job_reservation(configuration):
    settings, config = configuration
    store = QueueStore(replace(settings, max_pending=2), replace(config, max_work=40))
    submit(store, pages=9)
    with pytest.raises(ServiceBusy):
        submit(store, pages=9)
    submit(store, pages=1)
    assert store.stats()["work_units"] == 30


def test_expired_upload_does_not_delete_committed_job_input(configuration):
    settings, config = configuration
    now = [100.0]
    store = QueueStore(settings, config, clock=lambda: now[0])
    token = store.reserve_upload()
    key = f"inputs/{token}.pdf"
    store.submit(fingerprint="one", input_key=key, pages=1, tier="standard", encode_img=True)
    now[0] += config.upload_seconds + 11
    store.maintenance()
    assert key not in store.garbage()


def test_http_async_lifecycle_authentication_metrics_and_reuse(configuration):
    settings, config = configuration
    app = create_app(settings, queue_settings=config)
    headers = {"Authorization": "Bearer " + config.worker_token}
    from importlib.metadata import version

    with TestClient(app) as client:
        upload = {"file": ("private-name.pdf", pdf())}
        job = client.post("/jobs", files=upload).json()
        assert job["state"] == "queued" and "input_key" not in job
        assert client.get(job["result_url"]).status_code == 202
        assert client.post("/internal/worker/claim", json={}).status_code == 401
        claimed = client.post(
            "/internal/worker/claim",
            headers=headers,
            json={
                "worker_id": uuid4().hex,
                "model_revision": config.model_revision,
                "mineru_version": version("mineru"),
            },
        ).json()
        lease = {**headers, "X-Task-Lease": claimed["lease_token"]}
        assert client.get(f"/internal/jobs/{job['id']}/input", headers=lease).content.startswith(
            b"%PDF"
        )
        assert (
            client.post(f"/internal/jobs/{job['id']}/heartbeat", headers=lease).status_code == 200
        )
        done = client.post(
            f"/internal/jobs/{job['id']}/complete?pages=1",
            content=b"# Parsed content",
            headers=lease,
        )
        assert done.status_code == 200
        result = client.get(job["result_url"]).json()
        assert result["markdown"] == "# Parsed content"
        reused = client.post("/jobs", files=upload).json()
        assert reused["id"] == job["id"] and reused["reused"]
        metrics = client.get("/metrics")
        assert metrics.status_code == 200
        assert 'event="succeeded"' in metrics.text
        assert "private-name" not in metrics.text


def test_sync_waiter_timeout_does_not_cancel_durable_job(configuration):
    settings, config = configuration
    with TestClient(
        create_app(replace(settings, request_timeout=0.03), queue_settings=config)
    ) as client:
        response = client.post("/v2/parse/file", files={"file": ("test.pdf", pdf())})
        assert response.status_code == 504
        assert client.get("/health").json()["pending"] == 1
        assert client.post("/jobs", files={"file": ("test.pdf", pdf())}).json()["reused"]


def test_both_submission_routes_reject_documents_exceeding_work_budget(configuration):
    settings, config = configuration
    app = create_app(settings, queue_settings=replace(config, max_work=2))
    with TestClient(app) as client:
        for route in ("/jobs", "/v2/parse/file"):
            response = client.post(route, files={"file": ("sample.pdf", pdf())})
            assert response.status_code == 413
        assert client.get("/health").json()["pending"] == 0


def test_idempotency_conflict_through_http(configuration):
    settings, config = configuration
    with TestClient(create_app(settings, queue_settings=config)) as client:
        headers = {"Idempotency-Key": "example-key"}
        assert (
            client.post("/jobs", headers=headers, files={"file": ("a.pdf", pdf())}).status_code
            == 202
        )
        assert (
            client.post("/jobs", headers=headers, files={"file": ("b.pdf", pdf(2))}).status_code
            == 409
        )


def test_upload_capacity_is_global_before_body_is_read(configuration):
    settings, config = configuration
    config = replace(config, max_uploads=1)
    store = QueueStore(settings, config)
    token = store.reserve_upload()
    read = threading.Event()

    async def scenario():
        app = create_app(settings, queue_settings=config)

        async def body():
            read.set()
            yield b"not even a multipart body"

        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.post("/jobs", content=body())
                assert response.status_code == 503

    try:
        asyncio.run(scenario())
        assert not read.is_set()
    finally:
        store.release_upload(token)


def test_expired_upload_cannot_publish_a_job(configuration):
    settings, config = configuration
    now = [100.0]
    store = QueueStore(settings, config, clock=lambda: now[0])
    token = store.reserve_upload()
    now[0] += config.upload_seconds + 11
    with pytest.raises(ServiceBusy):
        store.submit(
            fingerprint="one",
            input_key=f"inputs/{token}.pdf",
            pages=1,
            tier="standard",
            encode_img=True,
            upload_token=token,
        )
    assert store.stats()["queued"] == 0


def test_sync_wait_budget_survives_upload_release(configuration):
    settings, config = configuration
    store = QueueStore(settings, replace(config, max_sync_waiters=1))
    token = store.reserve_upload(True)
    store.release_upload(token)
    with pytest.raises(ServiceBusy):
        store.reserve_upload(True)
    asynchronous = store.reserve_upload(False)
    store.release_upload(asynchronous)
    store.release_waiter(token)
    following = store.reserve_upload(True)
    store.release_upload(following)
    store.release_waiter(following)


def test_chunked_multipart_size_limit_is_enforced(configuration):
    settings, config = configuration

    async def scenario():
        app = create_app(replace(settings, max_upload_bytes=100), queue_settings=config)

        async def body():
            yield (
                b"--boundary\r\nContent-Disposition: form-data; "
                b'name="file"; filename="x.pdf"\r\n\r\n'
            )
            for _ in range(20):
                yield b"a" * 100000
            yield b"\r\n--boundary--\r\n"

        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.post(
                    "/jobs",
                    content=body(),
                    headers={"Content-Type": "multipart/form-data; boundary=boundary"},
                )
                assert response.status_code == 413

    asyncio.run(scenario())


def test_large_result_does_not_evict_eligible_smaller_cached_result(configuration):
    settings, config = configuration
    store = QueueStore(replace(settings, cache_bytes=10), config)
    small, _ = submit(store, "small")
    attempt = store.claim(uuid4().hex)
    assert store.complete(small["id"], attempt["lease_token"], 1, {}, result_bytes=5)
    large, _ = submit(store, "large")
    attempt = store.claim(uuid4().hex)
    assert store.complete(large["id"], attempt["lease_token"], 1, {}, result_bytes=20)
    cached, reused = submit(store, "small")
    assert reused and cached["id"] == small["id"]
    fresh, reused = submit(store, "large")
    assert not reused and fresh["id"] != large["id"]


def test_cache_budget_is_global_and_eviction_preserves_task_results(configuration):
    settings, config = configuration
    now = [100.0]
    store = QueueStore(replace(settings, cache_bytes=10), config, clock=lambda: now[0])
    completed = []
    for fingerprint in ("old", "middle", "new"):
        job, _ = submit(store, fingerprint)
        attempt = store.claim(uuid4().hex)
        assert store.complete(job["id"], attempt["lease_token"], 1, {}, result_bytes=4)
        completed.append(store.get(job["id"]))
        now[0] += 1
    old, middle, new = completed
    assert store.get(old["id"])["dedup_key"] is None
    assert store.get(middle["id"])["dedup_key"] == "middle"
    assert store.get(new["id"])["dedup_key"] == "new"
    assert store.get(old["id"])["output_key"] == old["output_key"]
    assert old["output_key"] not in store.garbage()
    fresh, reused = submit(store, "old")
    assert not reused and fresh["id"] != old["id"]


def test_distinct_idempotency_keys_can_share_one_global_parse(configuration):
    settings, config = configuration
    store = QueueStore(settings, config)
    first, _ = submit(store, "identical", idempotency_key="caller-one")
    second, reused = submit(store, "identical", idempotency_key="caller-two")
    assert reused and second["id"] == first["id"]
    with pytest.raises(IdempotencyConflict):
        submit(store, "other-input", idempotency_key="caller-two")


def test_transfer_capacity_is_global(configuration):
    settings, config = configuration
    config = replace(config, max_transfers=1)
    first = QueueStore(settings, config)
    second = QueueStore(settings, config)
    token = first.reserve_transfer()
    with pytest.raises(ServiceBusy):
        second.reserve_transfer()
    first.release_transfer(token)
    following = second.reserve_transfer()
    second.release_transfer(following)


def test_online_backup_and_limit_update_preserve_jobs(configuration, tmp_path, monkeypatch):
    import sys

    from app.queue_admin import main

    settings, config = configuration
    store = QueueStore(settings, config)
    job, _ = submit(store)
    destination = tmp_path / "backup.sqlite3"
    monkeypatch.setattr(
        sys, "argv", ["queue_admin", "--database", config.database, "backup", str(destination)]
    )
    main()
    restored = QueueStore(settings, replace(config, database=str(destination)))
    assert restored.get(job["id"])["state"] == "queued"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "queue_admin",
            "--database",
            config.database,
            "limits",
            "--max-pending",
            "1",
            "--max-work",
            "50",
            "--max-uploads",
            "1",
        ],
    )
    main()
    assert store.get(job["id"])["state"] == "queued"
    with pytest.raises(ServiceBusy):
        submit(store)


def test_unfixed_sqlite_is_rejected(monkeypatch):
    import sqlite3

    from app.sqlite_runtime import ensure_safe_wal

    monkeypatch.setattr(sqlite3, "sqlite_version_info", (3, 51, 2))
    with pytest.raises(RuntimeError, match="Concurrent WAL"):
        ensure_safe_wal()
