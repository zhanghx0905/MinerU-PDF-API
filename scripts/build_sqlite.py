"""Build a pinned SQLite runtime containing the concurrent WAL-reset fix."""

import argparse
import hashlib
import shutil
import subprocess
import tempfile
import urllib.request
import zipfile
from pathlib import Path

VERSION = "3510300"
URL = f"https://www.sqlite.org/2026/sqlite-amalgamation-{VERSION}.zip"
SHA256 = "acb1e6f5d832484bf6d32b681e858c38add8b2acdfd42ac5df24b8afb46552b4"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("artifacts/sqlite"))
    parser.add_argument("--url", default=URL)
    parser.add_argument("--source", type=Path, help="Optional offline source archive")
    args = parser.parse_args()
    compiler = shutil.which("cc")
    if not compiler:
        parser.error("A C compiler is required to build the SQLite runtime")
    with tempfile.TemporaryDirectory(prefix="sqlite-build-") as temporary:
        root = Path(temporary)
        archive = args.source or root / "source.zip"
        if not args.source:
            with urllib.request.urlopen(args.url, timeout=60) as response:
                archive.write_bytes(response.read())
        if hashlib.sha256(archive.read_bytes()).hexdigest() != SHA256:
            raise SystemExit("SQLite source checksum mismatch")
        with zipfile.ZipFile(archive) as source:
            (root / "sqlite3.c").write_bytes(
                source.read(f"sqlite-amalgamation-{VERSION}/sqlite3.c")
            )
        output = args.output.resolve() / "lib"
        output.mkdir(parents=True, exist_ok=True)
        target = output / "libsqlite3.so.0"
        subprocess.run(
            [
                compiler,
                "-O2",
                "-fPIC",
                "-shared",
                "-DSQLITE_THREADSAFE=1",
                "-Wl,-soname,libsqlite3.so.0",
                str(root / "sqlite3.c"),
                "-o",
                str(target),
                "-ldl",
                "-lm",
                "-lpthread",
            ],
            check=True,
        )
        print("SQLite 3.51.3 runtime built and source checksum verified")


if __name__ == "__main__":
    main()
