"""Bounded-label metrics and optional SDK diagnostic timings."""

import json
import re
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from time import perf_counter

from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, HistogramMetricFamily

STAGES = frozenset(
    {
        "api.upload",
        "api.validate",
        "storage.input",
        "storage.result",
        "queue.wait",
        "sdk.total",
        "markdown.render",
        "job.total",
        "worker.download",
        "model.initialize",
        "vlm.http",
        "vlm.queue",
        *(
            "pdf." + name
            for name in (
                "prepare",
                "cleanup",
                "geometry",
                "ocr_detection",
                "ocr_recognition",
                "text_fill",
                "formulas",
                "table_recognition",
                "formula_numbers",
                "render",
                "layout",
                "table_orientation",
                "native_tables",
                "seals",
                "image_assets",
            )
        ),
    }
)
_current = ContextVar("mineru_stage_timings", default=None)
_installed = False
_sink_lock = threading.Lock()
_pattern = re.compile(r"MinerU stage: name=([a-z_.]+), elapsed_s=([0-9.]+), pid=\d+")


def _sink(message):
    target = _current.get()
    if target is None:
        return
    match = _pattern.fullmatch(message.record["message"])
    if match and match[1] in STAGES:
        values, lock = target
        with lock:
            values[match[1]] = values.get(match[1], 0) + float(match[2])


@contextmanager
def record_sdk_stages(timings):
    global _installed
    from loguru import logger

    with _sink_lock:
        if not _installed:
            logger.add(_sink, filter=lambda record: record["name"] == "mineru.utils.timing")
            _installed = True
    token = _current.set((timings, threading.Lock()))
    try:
        yield
    finally:
        _current.reset(token)


@contextmanager
def timed(timings, stage):
    started = perf_counter()
    try:
        yield
    finally:
        timings[stage] = timings.get(stage, 0) + perf_counter() - started


class QueueCollector:
    def __init__(self, store):
        self.store = store

    def collect(self):
        from .jobs import BUCKETS

        rows, counters = self.store.metric_rows()
        durations = HistogramMetricFamily(
            "mineru_stage_seconds",
            "Stage wall time; nested SDK stages overlap",
            labels=["tier", "stage"],
        )
        for row in rows:
            counts = json.loads(row["buckets"])
            buckets = [
                ["+Inf" if bound == float("inf") else str(bound), count]
                for bound, count in zip(BUCKETS, counts, strict=True)
            ]
            durations.add_metric([row["tier"], row["stage"]], buckets, row["total"])
        yield durations
        events = CounterMetricFamily("mineru_jobs", "Durable coordinator events", labels=["event"])
        for row in counters:
            events.add_metric([row["event"]], row["value"])
        yield events
        stats = self.store.stats()
        queued = GaugeMetricFamily("mineru_tasks", "Tasks by state", labels=["state"])
        for state in ("queued", "running", "succeeded", "failed"):
            queued.add_metric([state], stats[state])
        yield queued
        for name in ("work_units", "oldest_wait_seconds", "workers"):
            metric = GaugeMetricFamily("mineru_" + name, "Coordinator " + name)
            metric.add_metric([], stats[name])
            yield metric


def render_metrics(store):
    registry = CollectorRegistry()
    registry.register(QueueCollector(store))
    return generate_latest(registry)
