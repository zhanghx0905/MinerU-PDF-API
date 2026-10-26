"""Measure uncached API latency and throughput using a repeatable PDF workload."""

import argparse
import asyncio
import hashlib
import json
import re
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
from pypdf import PdfReader


def percentile(values, quantile):
    values = sorted(values)
    position = (len(values) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


async def run(args):
    checks = json.loads(args.checks.read_text()) if args.checks else {}
    files = []
    for path in args.files:
        payload = path.read_bytes()
        files.append(
            {
                "name": path.name,
                "payload": payload,
                "pages": len(PdfReader(path).pages),
                "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )

    async with httpx.AsyncClient(
        base_url=args.url.rstrip("/"),
        timeout=args.timeout,
        trust_env=False,
        limits=httpx.Limits(max_connections=max(args.concurrency) + 2),
    ) as client:
        response = await client.get("/health")
        response.raise_for_status()
        health = response.json()
        if health["mineru_version"] != "4.0.12":
            raise RuntimeError(f"Unexpected MinerU version: {health}")

        async def request(file):
            started = time.perf_counter()
            result = {"file": file["name"], "pages": file["pages"]}
            try:
                response = await client.post(
                    "/v2/parse/file",
                    params={
                        "disable_cache": "true",
                        "encode_img": str(args.encode_img).lower(),
                        **({"tier": args.tier} if args.tier else {}),
                    },
                    files={"file": (file["name"], file["payload"], "application/pdf")},
                )
                response.raise_for_status()
                data = response.json()
                if not data["success"] or data["pages"] != file["pages"]:
                    raise ValueError(f"Incorrect parse response: {data.get('message')}")
                if not data["markdown"].strip():
                    raise ValueError("Empty Markdown")
                normalized = re.sub(r"\s+", "", data["markdown"]).casefold()
                for expected in checks.get(file["name"], []):
                    if re.sub(r"\s+", "", expected).casefold() not in normalized:
                        raise ValueError(f"Missing expected text: {expected}")
                result.update(
                    success=True,
                    checked_keywords=checks.get(file["name"], []),
                    server_seconds=data["duration"],
                    markdown_bytes=len(data["markdown"].encode()),
                )
            except (httpx.HTTPError, ValueError, KeyError) as exc:
                result.update(success=False, error=str(exc))
            result["seconds"] = time.perf_counter() - started
            return result

        # Warm every input, recording cold/first-parse latency separately.
        warmup = []
        for file in files:
            result = await request(file)
            warmup.append(result)
            print("warmup", json.dumps(result), flush=True)
            if not result["success"] and not args.allow_failures:
                raise RuntimeError("Warmup failed; benchmark aborted")

        levels = []
        for concurrency in args.concurrency:
            queue = asyncio.Queue()
            for index in range(args.requests):
                queue.put_nowait(files[index % len(files)])
            measurements = []

            async def worker(queue=queue, measurements=measurements):
                while not queue.empty():
                    file = queue.get_nowait()
                    measurements.append(await request(file))

            started = time.perf_counter()
            await asyncio.gather(*(worker() for _ in range(concurrency)))
            elapsed = time.perf_counter() - started
            good = [r for r in measurements if r["success"]]
            latencies = [r["seconds"] for r in good]
            pages = sum(r["pages"] for r in good)
            summary = {
                "concurrency": concurrency,
                "requests": len(measurements),
                "successes": len(good),
                "failures": len(measurements) - len(good),
                "wall_seconds": elapsed,
                "successful_pages": pages,
                "documents_per_minute": len(good) * 60 / elapsed,
                "pages_per_second": pages / elapsed,
                "mean_seconds": statistics.mean(latencies) if good else None,
                "p50_seconds": percentile(latencies, 0.5) if good else None,
                "p95_seconds": percentile(latencies, 0.95) if good else None,
                "seconds_per_page": sum(latencies) / pages if pages else None,
            }
            levels.append({"summary": summary, "requests": measurements})
            print(json.dumps(summary, indent=2), flush=True)

    report = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "url": args.url,
        "health": health,
        "tier": args.tier or health["tier"],
        "cache_disabled": True,
        "encode_img": args.encode_img,
        "files": [{k: v for k, v in f.items() if k != "payload"} for f in files],
        "warmup": warmup,
        "levels": levels,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    if not args.allow_failures and any(level["summary"]["failures"] for level in levels):
        raise SystemExit("Benchmark completed with failures; inspect the report")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("url")
    parser.add_argument("files", nargs="+", type=Path)
    parser.add_argument("--concurrency", nargs="+", type=int, default=[1, 2, 4, 8])
    parser.add_argument("--requests", type=int, default=16, help="Requests per concurrency level")
    parser.add_argument("--timeout", type=float, default=1200)
    parser.add_argument("--tier", choices=["flash", "basic", "standard", "advanced"])
    parser.add_argument(
        "--allow-failures", action="store_true", help="Record unsupported input modes"
    )
    parser.add_argument("--no-images", action="store_false", dest="encode_img", default=True)
    parser.add_argument("--output", type=Path, default=Path("artifacts/benchmark.json"))
    parser.add_argument(
        "--checks", type=Path, help="JSON mapping of file names to required phrases"
    )
    args = parser.parse_args()
    if args.requests < 1 or min(args.concurrency) < 1:
        parser.error("Request count and concurrency must be positive")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
