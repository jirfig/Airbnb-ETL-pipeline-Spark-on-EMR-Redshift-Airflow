from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
import pandas as pd
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

@pytest.fixture
def dim_model_output_dir():
    return DATA_DIR / "dim_model_airflow_temp"

@pytest.mark.skipif(not docker_available(), reason="docker not available")
def test_preprocess_data():
    # Run ETL step
    run_docker(["spark-submit", "apps/preprocess_data.py", "2021-01", "data"])
    # Validate its output using standalone script
    run_docker(["spark-submit", "scripts/validate_preprocess.py", "data"])

