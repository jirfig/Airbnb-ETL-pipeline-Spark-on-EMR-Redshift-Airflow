from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
COMPOSE_FILE = REPO_ROOT / "docker" / "spark" / "docker-compose.yml"


def docker_available() -> bool:
    return shutil.which("docker") is not None


def run_docker(cmd: list[str]) -> None:
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "exec", "spark"] + cmd,
        check=True,
    )


@pytest.fixture(scope="session")
def spark_service():
    """Start the Spark service using Docker Compose."""
    if not docker_available():
        pytest.skip("docker not available")
    
    # Start the service
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d", "--build"],
        check=True,
    )
    
    try:
        yield
    finally:
        # Cleanup: stop and remove containers
        subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "down"],
            check=True,
        ) 