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


@pytest.mark.order(1)
def test_preprocess_data(spark_service):
    run_docker(["spark-submit", "apps/preprocess_data.py", "2021-01", "data"])
    run_docker(["spark-submit", "scripts/validate_preprocess.py", "data"])


@pytest.mark.order(2)
def test_process_listings_hosts(spark_service):
    run_docker(["spark-submit", "apps/process_listings_hosts.py", "2021-01", "data"])
    run_docker(["spark-submit", "scripts/validate_listings_hosts.py", "data"])


@pytest.mark.order(3)
def test_process_reviews(spark_service):
    run_docker(["spark-submit", "apps/process_reviews.py", "2021-01", "data", "dummy"])
    run_docker(["spark-submit", "scripts/validate_reviews.py", "data"])


@pytest.mark.order(4)
def test_process_reviewers(spark_service):
    run_docker(["spark-submit", "apps/process_reviewers.py", "2021-01", "data"])
    run_docker(["spark-submit", "scripts/validate_reviewers.py", "data"])


@pytest.mark.order(5)
def test_process_weather(spark_service):
    run_docker(["spark-submit", "apps/process_weather.py", "2021-01", "data"])
    run_docker(["spark-submit", "scripts/validate_weather.py", "data"])

