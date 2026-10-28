"""Fixed-arrival-rate job load test; report contains anonymous aggregates only."""

import argparse
import asyncio
import io
import statistics
import time
from pathlib import Path

import httpx
from pypdf import PdfReader


def percentile(values, quantile):
    values = sorted(values)
    if not values:
        return 0
    index = (len(values) - 1) * quantile
    low = int(index)
    high = min(low + 1, len(values) - 1)
    return values[low] + (values[high] - values[low]) * (index - low)


async def run(args):
    files = []
    for path in args.files:
        payload = path.read_bytes()
        files.append((payload, len(PdfReader(io.BytesIO(payload)).pages)))
    jobs = {}
    submissions = set()
    counts = {
        "offered": 0,
        "accepted": 0,
        "rejected": 0,
        "transport_errors": 0,
        "client_dropped": 0,
        "failed": 0,
        "completed": 0,
        "completed_pages": 0,
    }
    latencies = []
    finished = []
    drained = asyncio.Event()
    offering_done = False
    started = time.perf_counter()
    async with httpx.AsyncClient(
        base_url=args.url.rstrip("/"),
        timeout=60,
        trust_env=False,
        limits=httpx.Limits(max_connections=args.max_inflight + 4),
    ) as client:

        async def submit(index):
            payload, pages = files[index % len(files)]
            sent = time.perf_counter()
            try:
                response = await client.post(
                    "/jobs",
                    params={"tier": args.tier, "disable_cache": "true"},
                    files={"file": ("sample.pdf", payload, "application/pdf")},
                )
                if response.status_code != 202:
                    counts["rejected"] += 1
                    return
                job_id = response.json()["id"]
                jobs[job_id] = (sent, pages)
                counts["accepted"] += 1
            except httpx.HTTPError:
                counts["transport_errors"] += 1

        async def poll():
            while True:
                for job_id in list(jobs):
                    try:
                        response = await client.get(f"/jobs/{job_id}")
                        response.raise_for_status()
                        state = response.json()["state"]
                        if state == "failed":
                            jobs.pop(job_id)
                            counts["failed"] += 1
                        elif state == "succeeded":
                            result = await client.get(f"/jobs/{job_id}/result")
                            result.raise_for_status()
                            body = result.json()
                            sent, pages = jobs.pop(job_id)
                            if (
                                not body.get("success")
                                or body.get("pages") != pages
                                or not body.get("markdown", "").strip()
                            ):
                                counts["failed"] += 1
                                continue
                            counts["completed"] += 1
                            counts["completed_pages"] += pages
                            now = time.perf_counter()
                            latencies.append(now - sent)
                            finished.append((now - started, pages))
                    except httpx.HTTPError:
                        pass
                if offering_done and not jobs:
                    drained.set()
                await asyncio.sleep(args.poll_seconds)

        poller = asyncio.create_task(poll())
        try:
            total = max(1, int(args.rate * args.duration))
            for index in range(total):
                await asyncio.sleep(max(0, started + index / args.rate - time.perf_counter()))
                counts["offered"] += 1
                if len(submissions) >= args.max_inflight:
                    counts["client_dropped"] += 1
                    continue
                task = asyncio.create_task(submit(index))
                submissions.add(task)
                task.add_done_callback(submissions.discard)
            await asyncio.gather(*list(submissions))
            offering_done = True
            if not jobs:
                drained.set()
            await asyncio.sleep(max(0, started + args.duration - time.perf_counter()))
            try:
                await asyncio.wait_for(drained.wait(), args.drain_seconds)
            except asyncio.TimeoutError:
                pass
        finally:
            poller.cancel()
            try:
                await poller
            except asyncio.CancelledError:
                pass
    elapsed = time.perf_counter() - started
    during_pages = sum(pages for when, pages in finished if when <= args.duration)
    summary = {
        **counts,
        "unfinished": len(jobs),
        "offered_rate": args.rate,
        "offer_window_seconds": args.duration,
        "wall_seconds_including_drain": round(elapsed, 3),
        "pages_per_second_including_drain": round(counts["completed_pages"] / elapsed, 4),
        "pages_per_second_in_offer_window": round(during_pages / args.duration, 4),
        "mean_response_seconds": round(statistics.mean(latencies), 3) if latencies else 0,
        "p95_response_seconds": round(percentile(latencies, 0.95), 3),
        "p99_response_seconds": round(percentile(latencies, 0.99), 3),
    }
    report = "# Fixed-arrival-rate load test\n\n| Metric | Value |\n| --- | ---: |\n"
    report += "".join(f"| {name} | {value} |\n" for name, value in summary.items())
    report += (
        "\nAPI result caching disabled. Throughput includes drain time; "
        "this is a finite load test.\n"
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report)
    print(report)
    if (
        not summary["accepted"]
        or (summary["rejected"] and not args.allow_rejections)
        or summary["failed"]
        or summary["unfinished"]
        or summary["transport_errors"]
        or summary["client_dropped"]
    ):
        raise SystemExit("Load test had failures, unfinished jobs, or generator errors")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url")
    parser.add_argument("files", nargs="+", type=Path)
    parser.add_argument("--rate", type=float, default=0.2, help="Submissions per second")
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--drain-seconds", type=float, default=600)
    parser.add_argument("--max-inflight", type=int, default=32)
    parser.add_argument("--poll-seconds", type=float, default=0.5)
    parser.add_argument("--allow-rejections", action="store_true")
    parser.add_argument(
        "--tier", choices=["flash", "basic", "standard", "advanced"], default="standard"
    )
    parser.add_argument("--output", type=Path, default=Path("artifacts/load-test.md"))
    args = parser.parse_args()
    if min(args.rate, args.duration, args.drain_seconds, args.max_inflight, args.poll_seconds) <= 0:
        parser.error("Load parameters must be positive")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
