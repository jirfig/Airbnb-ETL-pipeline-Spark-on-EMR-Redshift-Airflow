from __future__ import annotations

import pytest

from tests.conftest import run_docker


@pytest.mark.order(1)
def test_preprocess_data(spark_service):
    run_docker(["spark-submit", "apps/preprocess_data.py", "2021-01", "data"])
    run_docker(["spark-submit", "tests/validation/validate_preprocess.py", "data"])


@pytest.mark.order(2)
def test_process_listings_hosts(spark_service):
    run_docker(["spark-submit", "apps/process_listings_hosts.py", "2021-01", "data"])
    run_docker(["spark-submit", "tests/validation/validate_listings_hosts.py", "data"])


@pytest.mark.order(3)
def test_process_reviews(spark_service):
    run_docker(["spark-submit", "apps/process_reviews.py", "2021-01", "data", "dummy"])
    run_docker(["spark-submit", "tests/validation/validate_reviews.py", "data"])


@pytest.mark.order(4)
def test_process_reviewers(spark_service):
    run_docker(["spark-submit", "apps/process_reviewers.py", "2021-01", "data"])
    run_docker(["spark-submit", "tests/validation/validate_reviewers.py", "data"])


@pytest.mark.order(5)
def test_process_weather(spark_service):
    run_docker(["spark-submit", "apps/process_weather.py", "2021-01", "data"])
    run_docker(["spark-submit", "tests/validation/validate_weather.py", "data"])
