from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import boto3

DEFAULT_BUCKET_NAME = "airbnbprj-us"


@dataclass(frozen=True)
class DataPaths:
    raw_global_listings: str
    raw_city_listings: str
    raw_city_reviews: str
    raw_city_temperature: str
    raw_city_rain: str
    path_out_global_listings: str
    path_out_city_listings_data: str
    path_out_city_reviews_data: str
    path_out_city_temperature_data: str
    path_out_city_rain_data: str
    path_out_weather_stations: str
    dim_model_listings: str
    dim_model_hosts: str
    dim_model_reviews: str
    dim_model_reviewers: str
    dim_model_weather: str
    dim_model_listings_new: str
    dim_model_hosts_new: str
    dim_model_reviews_new: str
    dim_model_reviewers_new: str
    dim_model_weather_new: str
    dim_model_reviews_step1: str
    dim_model_reviews_step2: str


def join_path(base: str, *parts: str) -> str:
    """Join path parts for local or S3 URIs."""
    if base.startswith("s3://"):
        return "/".join([base.rstrip("/")] + list(parts))
    return str(Path(base, *parts))


def model_exists(path: str) -> bool:
    """Check if the given path exists on S3 or locally."""
    if path.startswith("s3://"):
        bucket, key = path.replace("s3://", "", 1).split("/", 1)
        s3_client = boto3.client("s3")
        response = s3_client.list_objects(Bucket=bucket, MaxKeys=1, Prefix=key)
        return "Contents" in response
    return Path(path).exists()


def build_paths(base_uri: str, scrape_year_month: str) -> DataPaths:
    """Return all data paths used in the ETL pipeline."""
    raw_data_folder = "raw"
    input_parquet_folder = "input_parquets_airflow"
    dim_model_folder = "dim_model_airflow"
    dim_model_folder_new = "dim_model_airflow_temp"


    path_global_listings = "airbnb-listings.csv"
    path_city_listings = f"cities/*/{scrape_year_month}/listings.csv"
    path_city_reviews = f"cities/*/{scrape_year_month}/reviews.csv"
    path_city_temperature = "weather/ECA_blend_tg/*.txt"
    path_city_rain = "weather/ECA_blend_rr/*.txt"

    return DataPaths(
        raw_global_listings=join_path(base_uri, raw_data_folder, path_global_listings),
        raw_city_listings=join_path(base_uri, raw_data_folder, path_city_listings),
        raw_city_reviews=join_path(base_uri, raw_data_folder, path_city_reviews),
        raw_city_temperature=join_path(base_uri, raw_data_folder, path_city_temperature),
        raw_city_rain=join_path(base_uri, raw_data_folder, path_city_rain),
        path_out_global_listings=join_path(base_uri, input_parquet_folder, "global_listings.parquet"),
        path_out_city_listings_data=join_path(
            base_uri,
            input_parquet_folder,
            f"city_listings/{scrape_year_month}/city_listings.parquet",
        ),
        path_out_city_reviews_data=join_path(
            base_uri,
            input_parquet_folder,
            f"city_reviews/{scrape_year_month}/city_reviews.parquet",
        ),
        path_out_city_temperature_data=join_path(base_uri, input_parquet_folder, "city_temperature.parquet"),
        path_out_city_rain_data=join_path(base_uri, input_parquet_folder, "city_rain.parquet"),
        path_out_weather_stations=join_path(base_uri, input_parquet_folder, "weather_stations.parquet"),
        dim_model_listings=join_path(base_uri, dim_model_folder, "listings.csv"),
        dim_model_hosts=join_path(base_uri, dim_model_folder, "hosts.csv"),
        dim_model_reviews=join_path(base_uri, dim_model_folder, "reviews.csv"),
        dim_model_reviewers=join_path(base_uri, dim_model_folder, "reviewers.csv"),
        dim_model_weather=join_path(base_uri, dim_model_folder, "weather.csv"),
        dim_model_listings_new=join_path(base_uri, dim_model_folder_new, "listings.csv"),
        dim_model_hosts_new=join_path(base_uri, dim_model_folder_new, "hosts.csv"),
        dim_model_reviews_new=join_path(base_uri, dim_model_folder_new, "reviews.csv"),
        dim_model_reviewers_new=join_path(base_uri, dim_model_folder_new, "reviewers.csv"),
        dim_model_weather_new=join_path(base_uri, dim_model_folder_new, "weather.csv"),
        dim_model_reviews_step1=join_path(base_uri, dim_model_folder_new, "reviews_step1.csv"),
        dim_model_reviews_step2=join_path(base_uri, dim_model_folder_new, "reviews_step2.csv"),
    )
