#!/usr/bin/env python3
"""Validate reviewers.csv produced by process_reviewers.py."""
from pathlib import Path
import sys
import logging
from pyspark.sql import SparkSession

EXPECTED_ROW_COUNTS = {
    "reviewers": None,
}

EXPECTED_COLS = {
    "reviewers": [
        'reviewer_id', 'reviewer_name', 'languages_spoken', 'last_updated'
    ],
}

FILES = {"reviewers": "reviewers.csv"}

def main(base_dir: Path) -> None:
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)

    root = base_dir / "dim_model_airflow_temp"
    assert root.exists(), f"Directory {root} not found"

    spark = (
        SparkSession.builder
        .appName("validate_reviewers")
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    try:
        path = root / FILES["reviewers"]
        assert path.exists(), f"Missing CSV directory: {path}"
        df = spark.read.csv(
            str(path), header=True, inferSchema=True, multiLine=True,
            escape='"', ignoreLeadingWhiteSpace=True
        )
        expected = EXPECTED_ROW_COUNTS["reviewers"]
        if expected is not None:
            actual = df.count()
            assert actual == expected, f"reviewers: expected {expected}, got {actual}"
        assert set(df.columns) == set(EXPECTED_COLS["reviewers"]), "reviewers: schema mismatch"
        print("OK")
    finally:
        spark.stop()

if __name__ == "__main__":
    base = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data")
    main(base)
