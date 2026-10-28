"""Online backup and explicit capacity updates for a coordinator-owned database."""

import argparse
import os
import sqlite3
from pathlib import Path

from .sqlite_runtime import ensure_safe_wal


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default=os.getenv("QUEUE_DATABASE", "data/queue.sqlite3"))
    commands = parser.add_subparsers(dest="command", required=True)
    backup = commands.add_parser("backup")
    backup.add_argument("destination", type=Path)
    limits = commands.add_parser("limits")
    limits.add_argument("--max-pending", type=int, required=True)
    limits.add_argument("--max-work", type=int, required=True)
    limits.add_argument("--max-uploads", type=int, required=True)
    args = parser.parse_args()
    ensure_safe_wal()
    if not Path(args.database).is_file():
        parser.error("Queue database does not exist")
    with sqlite3.connect(args.database, timeout=5) as connection:
        version = connection.execute("SELECT version FROM schema_version").fetchone()
        if not version or version[0] != 1:
            parser.error("Unsupported queue database schema")
        if args.command == "backup":
            if args.destination.resolve() == Path(args.database).resolve():
                parser.error("Backup destination must differ from the active database")
            args.destination.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(args.destination) as destination:
                connection.backup(destination)
            print("SQLite backup completed")
        else:
            values = (args.max_pending, args.max_work, args.max_uploads)
            if min(values) <= 0:
                parser.error("Queue limits must be positive")
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "UPDATE limits SET max_pending=?,max_work=?,max_uploads=? WHERE id=1", values
            )
            print("Queue limits updated; existing tasks were preserved")


if __name__ == "__main__":
    main()
