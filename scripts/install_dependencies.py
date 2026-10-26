"""Install the project's runtime/build dependencies, optionally from offline wheels."""

import os
import subprocess
import sys
from importlib.metadata import requires
from pathlib import Path

import tomllib


def main():
    with open("pyproject.toml", "rb") as source:
        project = tomllib.load(source)
    command = [sys.executable, "-m", "pip", "install", "--no-cache-dir"]
    wheelhouse = os.getenv("PIP_FIND_LINKS")
    if wheelhouse:
        command += ["--no-index", "--find-links", wheelhouse]
    else:
        command += ["--index-url", "https://pypi.tuna.tsinghua.edu.cn/simple"]
    subprocess.check_call(command + ["pip>=26,<27"])
    dependencies = project["project"]["dependencies"] + project["build-system"]["requires"]
    if os.getenv("SMALL_BACKEND") == "torch":
        dependencies += [
            line
            for line in Path("requirements-gpu.txt").read_text().splitlines()
            if line and not line.startswith("#")
        ]
    subprocess.check_call(command + dependencies)
    if os.getenv("SMALL_BACKEND") == "torch":
        from packaging.requirements import Requirement

        # Install the published Torch extra's exact requirements separately.
        # This reuses the preinstalled base distribution without an extras cycle.
        dependencies = []
        for raw in requires("mineru") or []:
            requirement = Requirement(raw)
            if (
                requirement.marker is not None
                and "extra" in str(requirement.marker)
                and requirement.marker.evaluate({"extra": "torch"})
            ):
                requirement.marker = None
                dependencies.append(str(requirement))
        # The supplied image ships an NCCL revision newer than Torch's exact
        # requirement. Satisfy the actual Torch metadata inside the API venv.
        dependencies += [
            requirement
            for requirement in requires("torch") or []
            if requirement.startswith("nvidia-nccl")
        ]
        subprocess.check_call(command + dependencies)


if __name__ == "__main__":
    main()
