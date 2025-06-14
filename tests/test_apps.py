from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from .dummy_data import generate_dummy_data

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "docker" / "spark" / "data"
COMPOSE_FILE = REPO_ROOT / "docker" / "spark" / "docker-compose.yml"


def docker_available() -> bool:
    return shutil.which("docker") is not None


def run_docker(cmd: list[str]) -> None:
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "run", "--rm", "spark"] + cmd,
        check=True,
    )


@pytest.fixture(scope="module")
def prepare_data(tmp_path_factory):
    if DATA_DIR.exists():
        shutil.rmtree(DATA_DIR)
    DATA_DIR.mkdir(parents=True)
    generate_dummy_data(DATA_DIR)
    return DATA_DIR


@pytest.mark.skipif(not docker_available(), reason="docker not available")
def test_run_apps(prepare_data):
    run_docker(["spark-submit", "apps/preprocess_data.py", "2021-01", "/data"])
    run_docker(["spark-submit", "apps/process_listings_hosts.py", "2021-01", "/data"])
    run_docker(["spark-submit", "apps/process_reviews.py", "2021-01", "/data"])
    run_docker(["spark-submit", "apps/process_reviewers.py", "2021-01", "/data"])
    run_docker(["spark-submit", "apps/process_weather.py", "2021-01", "/data"])

    assert (prepare_data / "dim_model_airflow_temp" / "listings.csv").exists()
    assert (prepare_data / "dim_model_airflow_temp" / "hosts.csv").exists()
    assert (prepare_data / "dim_model_airflow_temp" / "reviews.csv").exists()
    assert (prepare_data / "dim_model_airflow_temp" / "reviewers.csv").exists()
    assert (prepare_data / "dim_model_airflow_temp" / "weather.csv").exists()
