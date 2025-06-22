from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
COMPOSE_FILE = REPO_ROOT / "docker" / "spark" / "docker-compose.yml"

# Allow running tests without Docker by setting USE_DOCKER_TESTS=0
USE_DOCKER = os.getenv("USE_DOCKER_TESTS", "1") != "0"


def docker_available() -> bool:
    return shutil.which("docker") is not None


def run_docker(cmd: list[str]) -> None:
    """Run a command either inside the Docker container or locally."""
    if USE_DOCKER:
        subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "exec", "spark"] + cmd,
            check=True,
        )
    else:
        subprocess.run(cmd, check=True)


@pytest.fixture(scope="session")
def spark_service():
    """Prepare the Spark service for tests."""
    if USE_DOCKER:
        if not docker_available():
            pytest.skip("docker not available")

        subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d", "--build"],
            check=True,
        )

        try:
            yield
        finally:
            subprocess.run(
                ["docker", "compose", "-f", str(COMPOSE_FILE), "down"],
                check=True,
            )
    else:
        yield
