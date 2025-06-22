from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = REPO_ROOT / "docker" / "spark" / "docker-compose.yml"


def docker_available() -> bool:
    return shutil.which("docker") is not None


def run_docker(cmd: list[str]) -> None:
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "exec",
            "spark",
        ]
        + cmd,
        check=True,
    )


@pytest.fixture(scope="session")
def spark_service():
    if not docker_available():
        pytest.skip("docker not available")

    subprocess.run(["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d", "--build"], check=True)
    try:
        yield
    finally:
        subprocess.run(["docker", "compose", "-f", str(COMPOSE_FILE), "down"], check=True)


def test_transformations(spark_service):
    run_docker(["python", "tests/check_transforms.py"])
