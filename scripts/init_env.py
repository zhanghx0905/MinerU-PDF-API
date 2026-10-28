"""Create a private local deployment configuration without printing credentials."""

import argparse
import secrets
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", type=Path, default=Path(".env"))
    args = parser.parse_args()
    source = args.file.read_text() if args.file.exists() else Path(".env.example").read_text()
    lines = source.splitlines()
    found = False
    for index, line in enumerate(lines):
        if line.startswith("WORKER_API_TOKEN="):
            found = True
            value = line.partition("=")[2].strip()
            if not value or value.startswith("replace-"):
                lines[index] = "WORKER_API_TOKEN=" + secrets.token_hex(32)
    if not found:
        lines.append("WORKER_API_TOKEN=" + secrets.token_hex(32))
    args.file.write_text("\n".join(lines) + "\n")
    args.file.chmod(0o600)
    print("Local environment initialized; existing deployment values were preserved")


if __name__ == "__main__":
    main()
