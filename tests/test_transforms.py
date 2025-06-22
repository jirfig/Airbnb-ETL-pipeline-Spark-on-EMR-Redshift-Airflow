from __future__ import annotations

import pytest
from tests.conftest import run_docker


def test_transformations(spark_service):
    run_docker(["python", "-m", "tests.check_transforms"])
