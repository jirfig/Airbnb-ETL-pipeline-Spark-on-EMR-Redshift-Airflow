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
        ["docker", "compose", "-f", str(COMPOSE_FILE), "run", "--rm", "spark"] + cmd,
        check=True,
    )



@pytest.fixture(scope="session")
def preprocess_data():
    run_docker(["spark-submit", "apps/preprocess_data.py", "2021-01", "data"])
    run_docker(["spark-submit", "scripts/validate_preprocess.py", "data"])
    yield


@pytest.fixture(scope="session")
def process_listings_hosts(preprocess_data):
    run_docker(["spark-submit", "apps/process_listings_hosts.py", "2021-01", "data"])
    run_docker(["spark-submit", "scripts/validate_listings_hosts.py", "data"])
    yield


@pytest.fixture(scope="session")
def process_reviews(process_listings_hosts):
    run_docker(["spark-submit", "apps/process_reviews.py", "2021-01", "data"])
    run_docker(["spark-submit", "scripts/validate_reviews.py", "data"])
    yield


@pytest.fixture(scope="session")
def process_reviewers(process_reviews):
    run_docker(["spark-submit", "apps/process_reviewers.py", "2021-01", "data"])
    run_docker(["spark-submit", "scripts/validate_reviewers.py", "data"])
    yield


@pytest.fixture(scope="session")
def process_weather(preprocess_data):
    run_docker(["spark-submit", "apps/process_weather.py", "2021-01", "data"])
    run_docker(["spark-submit", "scripts/validate_weather.py", "data"])
    yield

@pytest.mark.skipif(not docker_available(), reason="docker not available")
def test_preprocess_data(preprocess_data):
    pass


@pytest.mark.skipif(not docker_available(), reason="docker not available")
def test_process_listings_hosts(process_listings_hosts):
    pass


@pytest.mark.skipif(not docker_available(), reason="docker not available")
def test_process_reviews(process_reviews):
    pass


@pytest.mark.skipif(not docker_available(), reason="docker not available")
def test_process_reviewers(process_reviewers):
    pass


@pytest.mark.skipif(not docker_available(), reason="docker not available")
def test_process_weather(process_weather):
    pass

