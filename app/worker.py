"""GPU workers use authenticated HTTP; the SQLite file stays on the coordinator."""

import argparse
import json
import logging
import os
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from importlib.metadata import version
from uuid import uuid4

import httpx

from .config import Settings
from .engine import MinerUEngine
from .queue_config import QueueSettings

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, settings, queue_settings, engine=None, client=None, fatal_exit=os._exit):
        if not queue_settings.worker_token:
            raise ValueError("WORKER_API_TOKEN is required")
        os.environ.setdefault("MINERU_PROFILE_STAGES", "true")
        self.settings = settings
        self.config = queue_settings
        self.engine = engine or MinerUEngine(settings.tier)
        self.client = client or httpx.Client(
            base_url=queue_settings.worker_api_url.rstrip("/"),
            headers={"Authorization": "Bearer " + queue_settings.worker_token},
            timeout=min(5, queue_settings.heartbeat_seconds / 2),
            trust_env=False,
            limits=httpx.Limits(max_connections=settings.parse_workers * 4 + 2),
        )
        self.id = uuid4().hex
        self.stop = threading.Event()
        self.fatal_exit = fatal_exit
        self.max_run_seconds = float(os.getenv("WORKER_MAX_RUN_SECONDS", settings.request_timeout))

    def claim(self):
        response = self.client.post(
            "/internal/worker/claim",
            json={
                "worker_id": self.id,
                "mineru_version": version("mineru"),
                "model_revision": self.config.model_revision,
            },
        )
        response.raise_for_status()
        return response.json() if response.status_code != 204 else None

    def execute(self, job):
        job_id, token = job["id"], job["lease_token"]
        headers = {"X-Task-Lease": token}
        done = threading.Event()
        lost = threading.Event()
        started = time.monotonic()
        deadline = [started + job["lease_seconds"] * 0.8]

        def heartbeat():
            while not done.wait(self.config.heartbeat_seconds):
                try:
                    response = self.client.post(
                        f"/internal/jobs/{job_id}/heartbeat", headers=headers
                    )
                    response.raise_for_status()
                    deadline[0] = time.monotonic() + response.json()["lease_seconds"] * 0.8
                except Exception as exc:
                    if (
                        isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 409
                    ) or time.monotonic() >= deadline[0]:
                        lost.set()
                if lost.is_set() or time.monotonic() - started > self.max_run_seconds:
                    # A native GPU call cannot safely be cancelled as a Python thread.
                    # Exit this worker process before its stale work can keep consuming a GPU.
                    logger.error("Worker lease or execution deadline expired")
                    self.fatal_exit(70)
                    return

        monitor = threading.Thread(target=heartbeat, daemon=True)
        monitor.start()
        try:
            download_started = time.perf_counter()
            response = self.client.get(
                f"/internal/jobs/{job_id}/input", headers=headers, timeout=60
            )
            response.raise_for_status()
            payload = response.content
            download_seconds = time.perf_counter() - download_started
            result = self.engine.parse(payload, job["encode_img"], job["tier"])
            if lost.is_set():
                return
            content = result.markdown.encode("utf-8")
            if len(content) > job["max_result_bytes"]:
                raise ValueError("Result exceeds coordinator limit")
            timings = {**result.timings, "worker.download": download_seconds}
            # Commit is idempotent for this lease, including a lost HTTP response.
            for attempt in range(3):
                try:
                    response = self.client.post(
                        f"/internal/jobs/{job_id}/complete",
                        content=content,
                        params={"pages": result.pages},
                        headers={
                            **headers,
                            "Content-Type": "text/markdown; charset=utf-8",
                            "X-Parse-Timings": json.dumps(timings),
                        },
                        timeout=60,
                    )
                    response.raise_for_status()
                    return
                except httpx.TransportError:
                    if attempt == 2:
                        raise
        except Exception as exc:
            logger.warning("Task attempt failed: %s", type(exc).__name__)
            if not lost.is_set():
                try:
                    response = self.client.post(f"/internal/jobs/{job_id}/fail", headers=headers)
                    response.raise_for_status()
                except Exception:
                    pass  # The coordinator recovers the task after its lease expires.
        finally:
            done.set()
            monitor.join(timeout=self.config.heartbeat_seconds + 6)

    def _slot(self):
        while not self.stop.is_set():
            try:
                job = self.claim()
                if job:
                    self.execute(job)
                    continue
            except Exception as exc:
                logger.warning("Worker coordinator request failed: %s", type(exc).__name__)
            self.stop.wait(self.config.poll_seconds)

    def run(self):
        try:
            if self.settings.preload_models:
                self.engine.warmup()
            with ThreadPoolExecutor(max_workers=self.settings.parse_workers) as pool:
                futures = [pool.submit(self._slot) for _ in range(self.settings.parse_workers)]
                for future in futures:
                    future.result()
        finally:
            self.client.close()
            self.engine.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    worker = Worker(Settings.from_env(), QueueSettings.from_env())
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_args: worker.stop.set())
    worker.run()


if __name__ == "__main__":
    main()
