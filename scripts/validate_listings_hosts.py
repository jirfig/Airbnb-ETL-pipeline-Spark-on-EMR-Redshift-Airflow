#!/usr/bin/env python3
"""Validate listings.csv and hosts.csv produced by process_listings_hosts.py."""
from pathlib import Path
import sys
import logging
from pyspark.sql import SparkSession

EXPECTED_ROW_COUNTS = {
    "listings": None,
    "hosts": None,
}

EXPECTED_COLS = {
    "listings": [
        'accommodates', 'amenities', 'availability_30', 'availability_365', 'availability_60',
        'availability_90', 'bathrooms', 'bathrooms_text', 'bedrooms', 'beds',
        'calculated_host_listings_count', 'calculated_host_listings_count_entire_homes',
        'calculated_host_listings_count_private_rooms', 'calculated_host_listings_count_shared_rooms',
        'calendar_last_scraped', 'calendar_updated', 'city', 'description', 'first_review',
        'has_availability', 'host_id', 'listing_id', 'instant_bookable', 'last_review',
        'last_scraped', 'latitude', 'license', 'listing_url', 'longitude',
        'maximum_maximum_nights', 'maximum_minimum_nights', 'maximum_nights', 'maximum_nights_avg_ntm',
        'minimum_maximum_nights', 'minimum_minimum_nights', 'minimum_nights', 'minimum_nights_avg_ntm',
        'name', 'neighborhood_overview', 'neighbourhood', 'neighbourhood_cleansed',
        'neighbourhood_group_cleansed', 'number_of_reviews', 'number_of_reviews_l30d',
        'number_of_reviews_ltm', 'picture_url', 'price', 'property_type',
        'review_scores_accuracy', 'review_scores_checkin', 'review_scores_cleanliness',
        'review_scores_communication', 'review_scores_location', 'review_scores_rating',
        'review_scores_value', 'reviews_per_month', 'room_type', 'scrape_id',
        'scrape_month', 'scrape_year'
    ],
    "hosts": [
        'host_id', 'host_name', 'host_url', 'host_since', 'host_location', 'host_about',
        'host_response_time', 'host_response_rate', 'host_acceptance_rate', 'host_is_superhost',
        'host_thumbnail_url', 'host_picture_url', 'host_neighbourhood', 'host_listings_count',
        'host_total_listings_count', 'host_verifications', 'host_has_profile_pic',
        'host_identity_verified', 'last_scraped'
    ],
}

FILES = {
    "listings": "listings.csv",
    "hosts": "hosts.csv",
}

def main(base_dir: Path) -> None:
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)

    root = base_dir / "dim_model_airflow_temp"
    assert root.exists(), f"Directory {root} not found"

    spark = (
        SparkSession.builder
        .appName("validate_listings_hosts")
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    try:
        for key, fname in FILES.items():
            path = root / fname
            assert path.exists(), f"Missing CSV directory: {path}"
            df = spark.read.csv(
                str(path), header=True, inferSchema=True, multiLine=True,
                escape='"', ignoreLeadingWhiteSpace=True
            )
            expected = EXPECTED_ROW_COUNTS[key]
            if expected is not None:
                actual = df.count()
                assert actual == expected, f"{key}: expected {expected}, got {actual}"
            expected_cols = set(EXPECTED_COLS[key])
            actual_cols = set(df.columns)
            assert actual_cols == expected_cols, f"{key}: schema mismatch"
        print("OK")
    finally:
        spark.stop()

if __name__ == "__main__":
    base = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data")
    main(base)
