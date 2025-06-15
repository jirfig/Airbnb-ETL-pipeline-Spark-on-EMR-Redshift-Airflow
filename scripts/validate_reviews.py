#!/usr/bin/env python3
"""Validate reviews.csv produced by process_reviews.py."""
from pathlib import Path
import sys
import logging
from pyspark.sql import SparkSession

EXPECTED_ROW_COUNTS = {
    "reviews": None,
}

EXPECTED_COLS = {
    "reviews": [
        'review_id', 'reviewer_id', 'listing_id', 'host_id', 'weather_id',
        'date', 'reviewer_name', 'comments', 'comment_language', 'sentiment'
    ],
}

FILES = {"reviews": "reviews.csv"}

def main(base_dir: Path) -> None:
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)

    root = base_dir / "dim_model_airflow_temp"
    assert root.exists(), f"Directory {root} not found"

    spark = (
        SparkSession.builder
        .appName("validate_reviews")
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    try:
        path = root / FILES["reviews"]
        assert path.exists(), f"Missing CSV directory: {path}"
        df = spark.read.csv(
            str(path), header=True, inferSchema=True, multiLine=True,
            escape='"', ignoreLeadingWhiteSpace=True
        )
        expected = EXPECTED_ROW_COUNTS["reviews"]
        if expected is not None:
            actual = df.count()
            assert actual == expected, f"reviews: expected {expected}, got {actual}"
        assert set(df.columns) == set(EXPECTED_COLS["reviews"]), "reviews: schema mismatch"
        print("OK")
    finally:
        spark.stop()

if __name__ == "__main__":
    base = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data")
    main(base)
