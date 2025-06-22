#!/usr/bin/env python3
"""Validate weather.csv produced by process_weather.py."""
from pathlib import Path
import sys
import logging
from pyspark.sql import SparkSession

EXPECTED_ROW_COUNTS = {
    "weather": 17888,
}

EXPECTED_COLS = {
    "weather": [
        'weather_id', 'date', 'temperature', 'rain', 'city'
    ],
}

FILES = {"weather": "weather.csv"}

def main(base_dir: Path) -> None:
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)

    root = base_dir / "dim_model_airflow_temp"
    assert root.exists(), f"Directory {root} not found"

    spark = (
        SparkSession.builder
        .appName("validate_weather")
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    try:
        path = root / FILES["weather"]
        assert path.exists(), f"Missing CSV directory: {path}"
        df = spark.read.csv(
            str(path), header=True, inferSchema=True, multiLine=True,
            escape='"', ignoreLeadingWhiteSpace=True
        )
        actual = df.count()
        assert actual == EXPECTED_ROW_COUNTS["weather"], f"weather: expected {EXPECTED_ROW_COUNTS['weather']}, got {actual}"
        assert set(df.columns) == set(EXPECTED_COLS["weather"]), "weather: schema mismatch"
        print("OK")
    finally:
        spark.stop()

if __name__ == "__main__":
    base = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data")
    main(base)
