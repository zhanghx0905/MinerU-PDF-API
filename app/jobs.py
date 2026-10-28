"""SQLite WAL coordinator. Transactions contain metadata only, never model execution."""

import json
import math
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from .service import ServiceBusy
from .sqlite_runtime import ensure_safe_wal

BUCKETS = (0.01, 0.1, 0.5, 1, 5, 20, 60, 300, math.inf)
TIERS = {"flash": 1, "basic": 2, "standard": 3, "advanced": 6}
ACTIVE = ("queued", "running")


class IdempotencyConflict(Exception):
    pass


class LeaseLost(Exception):
    pass


class WorkBudgetExceeded(ValueError):
    pass


class QueueStore:
    def __init__(self, settings, queue_settings, clock=time.time):
        ensure_safe_wal()
        self.settings = settings
        self.config = queue_settings
        self.clock = clock
        Path(queue_settings.database).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as connection:
            mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            if mode != "wal":
                raise ValueError("Queue requires a persistent local SQLite WAL database")
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                    dedup_key TEXT UNIQUE, idempotency_key TEXT UNIQUE,
                    input_key TEXT NOT NULL, output_key TEXT,
                    tier TEXT NOT NULL, encode_img INTEGER NOT NULL,
                    pages INTEGER NOT NULL, cost INTEGER NOT NULL, lane TEXT NOT NULL,
                    state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                    lease_token TEXT, completed_token TEXT, lease_until REAL, worker_id TEXT,
                    created REAL NOT NULL, queued REAL NOT NULL, available REAL NOT NULL,
                    started REAL, finished REAL, cache_until REAL,
                    error TEXT, result_bytes INTEGER NOT NULL DEFAULT 0,
                    timings TEXT NOT NULL DEFAULT '{}'
                );
                CREATE TABLE IF NOT EXISTS idempotency (
                    key TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, job_id TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idempotency_job ON idempotency(job_id);
                CREATE INDEX IF NOT EXISTS jobs_schedule ON jobs(state, available, queued);
                CREATE INDEX IF NOT EXISTS jobs_retention ON jobs(state, finished);
                CREATE INDEX IF NOT EXISTS jobs_cache_expiry ON jobs(cache_until)
                    WHERE dedup_key IS NOT NULL;
                CREATE TABLE IF NOT EXISTS uploads (id TEXT PRIMARY KEY, expires REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS waiters (id TEXT PRIMARY KEY, expires REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS transfers (
                    id TEXT PRIMARY KEY, expires REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS garbage (key TEXT PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS workers (id TEXT PRIMARY KEY, seen REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS metrics (
                    stage TEXT NOT NULL, tier TEXT NOT NULL, count INTEGER NOT NULL,
                    total REAL NOT NULL, buckets TEXT NOT NULL, PRIMARY KEY(stage, tier)
                );
                CREATE TABLE IF NOT EXISTS counters (
                    event TEXT PRIMARY KEY, value INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS limits (
                    id INTEGER PRIMARY KEY CHECK(id=1), max_pending INTEGER NOT NULL,
                    max_work INTEGER NOT NULL, max_uploads INTEGER NOT NULL
                );
            """)
        with self.transaction() as connection:
            version = connection.execute("SELECT version FROM schema_version").fetchone()
            if version is None:
                connection.execute("INSERT INTO schema_version VALUES (1)")
            elif version[0] != 1:
                raise ValueError("Unsupported queue schema version")
            values = (settings.max_pending, queue_settings.max_work, queue_settings.max_uploads)
            connection.execute("INSERT OR IGNORE INTO limits VALUES (1,?,?,?)", values)
            stored = connection.execute(
                "SELECT max_pending,max_work,max_uploads FROM limits WHERE id=1"
            ).fetchone()
            if tuple(stored) != values:
                raise ValueError("Coordinator processes must use identical queue limits")

    @contextmanager
    def connect(self):
        connection = sqlite3.connect(self.config.database, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute("PRAGMA synchronous=FULL")
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def transaction(self):
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    @staticmethod
    def _garbage(connection, key):
        if key:
            connection.execute("INSERT OR IGNORE INTO garbage VALUES (?)", (key,))

    @staticmethod
    def _counter(connection, event):
        connection.execute(
            "INSERT INTO counters VALUES (?,1) ON CONFLICT(event) DO UPDATE SET value=value+1",
            (event,),
        )

    def _observe(self, connection, tier, timings):
        for stage, value in timings.items():
            if not math.isfinite(value) or value < 0:
                continue
            old = connection.execute(
                "SELECT * FROM metrics WHERE stage=? AND tier=?", (stage, tier)
            ).fetchone()
            counts = json.loads(old["buckets"]) if old else [0] * len(BUCKETS)
            counts = [
                count + int(value <= bound) for count, bound in zip(counts, BUCKETS, strict=True)
            ]
            connection.execute(
                "INSERT OR REPLACE INTO metrics VALUES (?,?,?,?,?)",
                (
                    stage,
                    tier,
                    (old["count"] if old else 0) + 1,
                    (old["total"] if old else 0) + value,
                    json.dumps(counts),
                ),
            )

    def observe(self, tier, timings):
        with self.transaction() as connection:
            self._observe(connection, tier, timings)

    def _terminal(self, connection, job, error, now):
        self._garbage(connection, job["input_key"])
        if job["lease_token"]:
            self._garbage(connection, f"results/{job['lease_token']}.md")
        connection.execute(
            "UPDATE jobs SET state='failed',finished=?,error=?,dedup_key=NULL,"
            "lease_token=NULL,lease_until=NULL WHERE id=?",
            (now, error, job["id"]),
        )
        self._counter(connection, "failed")

    def _sweep(self, connection, now):
        for upload in connection.execute("SELECT id FROM uploads WHERE expires<=?", (now,)):
            key = f"inputs/{upload['id']}.pdf"
            if not connection.execute("SELECT 1 FROM jobs WHERE input_key=?", (key,)).fetchone():
                self._garbage(connection, key)
        connection.execute("DELETE FROM uploads WHERE expires<=?", (now,))
        connection.execute("DELETE FROM waiters WHERE expires<=?", (now,))
        connection.execute("DELETE FROM transfers WHERE expires<=?", (now,))
        for job in connection.execute(
            "SELECT * FROM jobs WHERE state='running' AND lease_until<=?", (now,)
        ).fetchall():
            self._retry(connection, job, "lease_expired", now)
        for job in connection.execute(
            "SELECT * FROM jobs WHERE state='queued' AND queued<?",
            (now - self.config.max_queue_seconds,),
        ).fetchall():
            self._terminal(connection, job, "queue_timeout", now)
        connection.execute(
            "UPDATE jobs SET dedup_key=NULL WHERE state='succeeded' "
            "AND dedup_key IS NOT NULL AND cache_until<=?",
            (now,),
        )
        for job in connection.execute(
            "SELECT * FROM jobs WHERE state IN ('succeeded','failed') AND finished<?",
            (now - self.config.retention_seconds,),
        ).fetchall():
            self._garbage(connection, job["input_key"])
            self._garbage(connection, job["output_key"])
            connection.execute("DELETE FROM idempotency WHERE job_id=?", (job["id"],))
            connection.execute("DELETE FROM jobs WHERE id=?", (job["id"],))
        connection.execute(
            "DELETE FROM workers WHERE seen<?", (now - self.config.lease_seconds * 2,)
        )

    def maintenance(self):
        with self.transaction() as connection:
            self._sweep(connection, self.clock())

    def reserve_upload(self, wait_for_result=False):
        now = self.clock()
        with self.transaction() as connection:
            self._sweep(connection, now)
            count = connection.execute("SELECT count(*) FROM uploads").fetchone()[0]
            limit = connection.execute("SELECT max_uploads FROM limits WHERE id=1").fetchone()[0]
            if count >= limit:
                raise ServiceBusy("Upload capacity is full; retry later")
            if wait_for_result:
                waiting = connection.execute("SELECT count(*) FROM waiters").fetchone()[0]
                if waiting >= self.config.max_sync_waiters:
                    raise ServiceBusy("Synchronous wait capacity is full")
            token = uuid4().hex
            if wait_for_result:
                connection.execute(
                    "INSERT INTO waiters VALUES (?,?)",
                    (token, now + self.config.upload_seconds + self.settings.request_timeout + 15),
                )
            connection.execute(
                "INSERT INTO uploads VALUES (?,?)", (token, now + self.config.upload_seconds + 10)
            )
            return token

    def reserve_transfer(self):
        with self.transaction() as connection:
            now = self.clock()
            connection.execute("DELETE FROM transfers WHERE expires<=?", (now,))
            if (
                connection.execute("SELECT count(*) FROM transfers").fetchone()[0]
                >= self.config.max_transfers
            ):
                raise ServiceBusy("Transfer capacity is full")
            token = uuid4().hex
            connection.execute(
                "INSERT INTO transfers VALUES (?,?)",
                (token, now + self.config.transfer_seconds + 10),
            )
            return token

    def release_transfer(self, token):
        with self.transaction() as connection:
            connection.execute("DELETE FROM transfers WHERE id=?", (token,))

    def renew_upload(self, token):
        with self.transaction() as connection:
            now = self.clock()
            return (
                connection.execute(
                    "UPDATE uploads SET expires=? WHERE id=? AND expires>?",
                    (now + self.config.upload_seconds + 10, token, now),
                ).rowcount
                == 1
            )

    def release_waiter(self, token):
        with self.transaction() as connection:
            connection.execute("DELETE FROM waiters WHERE id=?", (token,))

    def release_upload(self, token):
        with self.transaction() as connection:
            key = f"inputs/{token}.pdf"
            if not connection.execute("SELECT 1 FROM jobs WHERE input_key=?", (key,)).fetchone():
                self._garbage(connection, key)
            connection.execute("DELETE FROM uploads WHERE id=?", (token,))

    def submit(
        self,
        *,
        fingerprint,
        input_key,
        pages,
        tier,
        encode_img,
        disable_cache=False,
        idempotency_key=None,
        upload_token=None,
    ):
        now = self.clock()
        cost = pages * TIERS[tier]
        with self.transaction() as connection:
            self._sweep(connection, now)
            limits = connection.execute("SELECT * FROM limits WHERE id=1").fetchone()
            if cost > limits["max_work"]:
                raise WorkBudgetExceeded("Document exceeds the queue work budget")
            if (
                upload_token
                and not connection.execute(
                    "SELECT 1 FROM uploads WHERE id=? AND expires>?", (upload_token, now)
                ).fetchone()
            ):
                raise ServiceBusy("Upload reservation expired")
            if idempotency_key:
                existing = connection.execute(
                    "SELECT jobs.* FROM idempotency JOIN jobs ON jobs.id=idempotency.job_id "
                    "WHERE idempotency.key=?",
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if existing["fingerprint"] != fingerprint:
                        raise IdempotencyConflict("Idempotency key was used for different input")
                    self._counter(connection, "idempotency_hit")
                    return dict(existing), True
            if not disable_cache:
                existing = connection.execute(
                    "SELECT * FROM jobs WHERE dedup_key=?", (fingerprint,)
                ).fetchone()
                if existing:
                    if idempotency_key:
                        connection.execute(
                            "INSERT INTO idempotency VALUES (?,?,?)",
                            (idempotency_key, fingerprint, existing["id"]),
                        )
                    self._counter(
                        connection, "cache_hit" if existing["state"] == "succeeded" else "coalesced"
                    )
                    return dict(existing), True
            count, work = connection.execute(
                "SELECT count(*),coalesce(sum(cost),0) FROM jobs "
                "WHERE state IN ('queued','running')"
            ).fetchone()
            if count >= limits["max_pending"] or work + cost > limits["max_work"]:
                raise ServiceBusy("Parse queue is full; retry later")
            lane = "short" if pages <= 8 else "bulk"
            reserved = min(1, limits["max_pending"] - 1)
            if lane == "bulk" and count >= limits["max_pending"] - reserved:
                raise ServiceBusy("Bulk queue is full; retry later")
            job_id = uuid4().hex
            # Explicit independent keys may intentionally represent distinct jobs.
            dedup = fingerprint if not disable_cache else None
            connection.execute(
                "INSERT INTO jobs (id,fingerprint,dedup_key,idempotency_key,input_key,"
                "tier,encode_img,pages,cost,lane,state,created,queued,available) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,'queued',?,?,?)",
                (
                    job_id,
                    fingerprint,
                    dedup,
                    None,
                    input_key,
                    tier,
                    int(encode_img),
                    pages,
                    cost,
                    lane,
                    now,
                    now,
                    now,
                ),
            )
            if idempotency_key:
                connection.execute(
                    "INSERT INTO idempotency VALUES (?,?,?)", (idempotency_key, fingerprint, job_id)
                )
            self._counter(connection, "submitted")
            return dict(
                connection.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            ), False

    def claim(self, worker_id):
        now = self.clock()
        # Idle polling is read-only except for periodic liveness refreshes.
        with self.connect() as reader:
            seen = reader.execute("SELECT seen FROM workers WHERE id=?", (worker_id,)).fetchone()
            pending = reader.execute(
                "SELECT 1 FROM jobs WHERE state='queued' AND available<=? LIMIT 1", (now,)
            ).fetchone()
            if seen and seen[0] > now - min(5, self.config.lease_seconds / 3) and not pending:
                return None
        with self.transaction() as connection:
            self._sweep(connection, now)
            connection.execute("INSERT OR REPLACE INTO workers VALUES (?,?)", (worker_id, now))
            job = connection.execute(
                "SELECT * FROM jobs WHERE state='queued' AND available<=? "
                "ORDER BY CASE WHEN queued<? THEN 0 WHEN lane='short' THEN 1 ELSE 2 END,queued,id "
                "LIMIT 1",
                (now, now - 60),
            ).fetchone()
            if not job:
                return None
            token = uuid4().hex
            connection.execute(
                "UPDATE jobs SET state='running',attempts=attempts+1,lease_token=?,"
                "lease_until=?,started=?,worker_id=? WHERE id=?",
                (token, now + self.config.lease_seconds, now, worker_id, job["id"]),
            )
            self._observe(connection, job["tier"], {"queue.wait": now - job["queued"]})
            return dict(
                connection.execute("SELECT * FROM jobs WHERE id=?", (job["id"],)).fetchone()
            )

    def check_lease(self, job_id, token, connection=None):
        if connection is None:
            with self.connect() as owned:
                return self.check_lease(job_id, token, owned)
        row = connection.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row and row["state"] == "succeeded" and row["completed_token"] == token:
            return dict(row)
        if (
            not row
            or row["state"] != "running"
            or row["lease_token"] != token
            or row["lease_until"] <= self.clock()
        ):
            raise LeaseLost("Task lease is no longer valid")
        return dict(row)

    def heartbeat(self, job_id, token):
        with self.transaction() as connection:
            job = self.check_lease(job_id, token, connection)
            if job["state"] == "running":
                connection.execute(
                    "UPDATE workers SET seen=? WHERE id=?", (self.clock(), job["worker_id"])
                )
                connection.execute(
                    "UPDATE jobs SET lease_until=? WHERE id=?",
                    (self.clock() + self.config.lease_seconds, job_id),
                )
            return job

    def complete(self, job_id, token, pages, timings, result_bytes=0):
        key = f"results/{token}.md"
        with self.transaction() as connection:
            try:
                job = self.check_lease(job_id, token, connection)
            except LeaseLost:
                self._garbage(connection, key)
                return False
            if job["state"] == "succeeded":
                return True
            if pages != job["pages"]:
                self._garbage(connection, key)
                self._retry(connection, job, "page_mismatch", self.clock())
                return False
            now = self.clock()
            connection.execute(
                "UPDATE jobs SET state='succeeded',output_key=?,finished=?,cache_until=?,"
                "completed_token=?,lease_token=NULL,lease_until=NULL,error=NULL,timings=?,"
                "result_bytes=? WHERE id=?",
                (
                    key,
                    now,
                    now + self.settings.cache_ttl,
                    token,
                    json.dumps(timings),
                    result_bytes,
                    job_id,
                ),
            )
            cached = connection.execute(
                "SELECT id,result_bytes FROM jobs WHERE dedup_key IS NOT NULL "
                "AND state='succeeded' ORDER BY finished DESC"
            ).fetchall()
            cache_size = 0
            for row in cached:
                if (
                    not self.settings.cache_bytes
                    or cache_size + row["result_bytes"] > self.settings.cache_bytes
                ):
                    connection.execute("UPDATE jobs SET dedup_key=NULL WHERE id=?", (row["id"],))
                else:
                    cache_size += row["result_bytes"]
            self._observe(connection, job["tier"], timings)
            self._observe(connection, job["tier"], {"job.total": now - job["created"]})
            self._counter(connection, "succeeded")
            self._garbage(connection, job["input_key"])
            return True

    def _retry(self, connection, job, error, now):
        if job["lease_token"]:
            self._garbage(connection, f"results/{job['lease_token']}.md")
        if job["attempts"] >= self.config.max_attempts:
            self._terminal(connection, job, error, now)
        else:
            connection.execute(
                "UPDATE jobs SET state='queued',lease_token=NULL,lease_until=NULL,"
                "queued=?,available=?,error=? WHERE id=?",
                (now, now + min(2 ** job["attempts"], 30), error, job["id"]),
            )
            self._counter(connection, "retried")

    def fail(self, job_id, token):
        with self.transaction() as connection:
            job = self.check_lease(job_id, token, connection)
            if job["state"] == "running":
                self._retry(connection, job, "parse_error", self.clock())

    def get(self, job_id):
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            return dict(row) if row else None

    def stats(self):
        now = self.clock()
        with self.connect() as connection:
            states = {
                row[0]: row[1]
                for row in connection.execute("SELECT state,count(*) FROM jobs GROUP BY state")
            }
            work = connection.execute(
                "SELECT coalesce(sum(cost),0) FROM jobs WHERE state IN ('queued','running')"
            ).fetchone()[0]
            oldest = connection.execute(
                "SELECT min(queued) FROM jobs WHERE state='queued'"
            ).fetchone()[0]
            workers = connection.execute(
                "SELECT count(*) FROM workers WHERE seen>?", (now - self.config.lease_seconds * 2,)
            ).fetchone()[0]
            return {
                **{
                    state: states.get(state, 0)
                    for state in ("queued", "running", "succeeded", "failed")
                },
                "work_units": work,
                "oldest_wait_seconds": max(0, now - oldest) if oldest else 0,
                "workers": workers,
            }

    def metric_rows(self):
        with self.connect() as connection:
            return (
                [dict(row) for row in connection.execute("SELECT * FROM metrics")],
                [dict(row) for row in connection.execute("SELECT * FROM counters")],
            )

    def garbage(self):
        with self.connect() as connection:
            return [row[0] for row in connection.execute("SELECT key FROM garbage LIMIT 100")]

    def forget_garbage(self, key):
        with self.transaction() as connection:
            connection.execute("DELETE FROM garbage WHERE key=?", (key,))
