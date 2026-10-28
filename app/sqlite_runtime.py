"""SQLite version guard for concurrent WAL use."""

import sqlite3


def ensure_safe_wal():
    version = sqlite3.sqlite_version_info
    fixed_backport = (3, 44, 6) <= version < (3, 45, 0) or (3, 50, 7) <= version < (3, 51, 0)
    if version < (3, 51, 3) and not fixed_backport:
        raise RuntimeError(
            "Concurrent WAL requires SQLite 3.51.3+ or a fixed backport. "
            "Use the API image or build scripts/build_sqlite.py and set LD_PRELOAD."
        )
