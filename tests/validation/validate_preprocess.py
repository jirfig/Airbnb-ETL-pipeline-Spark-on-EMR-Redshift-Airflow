#!/usr/bin/env python3
"""
Validate the parquet files produced by apps/preprocess_data.py.
The script prints "OK" if everything matches the expected baseline and exits
with a non-zero status otherwise (via AssertionError).
"""

import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession

# ---------- expected metadata ----------
EXPECTED_ROW_COUNTS = {
    "global_listings": 494_954,
    "city_listings": 181_047,
    "city_reviews": 3_299_278,
    "city_temperature": 137_636,
    "city_rain": 168_347,
    "weather_stations": 4,
}

EXPECTED_COLS = {
    "global_listings": [
        "id",
        "listing_url",
        "scrape_id",
        "last_scraped",
        "name",
        "description",
        "neighborhood_overview",
        "picture_url",
        "host_id",
        "host_url",
        "host_name",
        "host_since",
        "host_location",
        "host_about",
        "host_response_time",
        "host_response_rate",
        "host_acceptance_rate",
        "host_thumbnail_url",
        "host_picture_url",
        "host_neighbourhood",
        "host_listings_count",
        "host_total_listings_count",
        "host_verifications",
        "neighbourhood",
        "neighbourhood_cleansed",
        "neighbourhood_group_cleansed",
        "city",
        "latitude",
        "longitude",
        "property_type",
        "room_type",
        "accommodates",
        "bathrooms",
        "bedrooms",
        "beds",
        "amenities",
        "price",
        "minimum_nights",
        "maximum_nights",
        "calendar_updated",
        "has_availability",
        "availability_30",
        "availability_60",
        "availability_90",
        "availability_365",
        "calendar_last_scraped",
        "number_of_reviews",
        "first_review",
        "last_review",
        "review_scores_rating",
        "review_scores_accuracy",
        "review_scores_cleanliness",
        "review_scores_checkin",
        "review_scores_communication",
        "review_scores_location",
        "review_scores_value",
        "license",
        "calculated_host_listings_count",
        "reviews_per_month",
        "scrape_year",
        "scrape_month",
    ],
    "city_listings": [
        "id",
        "listing_url",
        "scrape_id",
        "last_scraped",
        "name",
        "description",
        "neighborhood_overview",
        "picture_url",
        "host_id",
        "host_url",
        "host_name",
        "host_since",
        "host_location",
        "host_about",
        "host_response_time",
        "host_response_rate",
        "host_acceptance_rate",
        "host_is_superhost",
        "host_thumbnail_url",
        "host_picture_url",
        "host_neighbourhood",
        "host_listings_count",
        "host_total_listings_count",
        "host_verifications",
        "host_has_profile_pic",
        "host_identity_verified",
        "neighbourhood",
        "neighbourhood_cleansed",
        "neighbourhood_group_cleansed",
        "latitude",
        "longitude",
        "property_type",
        "room_type",
        "accommodates",
        "bathrooms",
        "bathrooms_text",
        "bedrooms",
        "beds",
        "amenities",
        "price",
        "minimum_nights",
        "maximum_nights",
        "minimum_minimum_nights",
        "maximum_minimum_nights",
        "minimum_maximum_nights",
        "maximum_maximum_nights",
        "minimum_nights_avg_ntm",
        "maximum_nights_avg_ntm",
        "calendar_updated",
        "has_availability",
        "availability_30",
        "availability_60",
        "availability_90",
        "availability_365",
        "calendar_last_scraped",
        "number_of_reviews",
        "number_of_reviews_ltm",
        "number_of_reviews_l30d",
        "first_review",
        "last_review",
        "review_scores_rating",
        "review_scores_accuracy",
        "review_scores_cleanliness",
        "review_scores_checkin",
        "review_scores_communication",
        "review_scores_location",
        "review_scores_value",
        "license",
        "instant_bookable",
        "calculated_host_listings_count",
        "calculated_host_listings_count_entire_homes",
        "calculated_host_listings_count_private_rooms",
        "calculated_host_listings_count_shared_rooms",
        "reviews_per_month",
        "city",
        "scrape_year",
        "scrape_month",
    ],
    "city_reviews": ["listing_id", "id", "date", "reviewer_id", "reviewer_name", "comments", "year", "month", "city"],
    "city_temperature": ["STAID", "SOUID", "DATE", "TG", "Q_TG"],
    "city_rain": ["STAID", "SOUID", "DATE", "RR", "Q_TG"],
    "weather_stations": ["STAID", "city"],
}

FILES = {
    "global_listings": "global_listings.parquet",
    "city_listings": "city_listings/2021-01/city_listings.parquet",
    "city_reviews": "city_reviews/2021-01/city_reviews.parquet",
    "city_temperature": "city_temperature.parquet",
    "city_rain": "city_rain.parquet",
    "weather_stations": "weather_stations.parquet",
}


def main(base_dir: Path) -> None:
    # silence Spark logs
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)

    parquet_root = base_dir / "input_parquets_airflow"
    assert parquet_root.exists(), f"Directory {parquet_root} not found"

    spark = (
        SparkSession.builder.appName("validate_preprocess")
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    try:
        for key, rel in FILES.items():
            path = parquet_root / rel
            assert path.exists(), f"Missing parquet file: {path}"
            df = spark.read.parquet(str(path))
            # row count
            expected_rows = EXPECTED_ROW_COUNTS[key]
            actual_rows = df.count()
            assert actual_rows == expected_rows, f"{key}: expected {expected_rows}, got {actual_rows}"
            # columns
            expected_cols = set(EXPECTED_COLS[key])
            actual_cols = set(df.columns)
            assert actual_cols == expected_cols, f"{key}: schema mismatch"
        print("OK")
    finally:
        spark.stop()


if __name__ == "__main__":
    base = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data")
    main(base)
