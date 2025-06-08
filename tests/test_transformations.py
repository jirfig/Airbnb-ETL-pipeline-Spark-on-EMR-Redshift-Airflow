import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from apps.transformations import (
    process_listings_hosts,
    process_reviewers,
    process_reviews,
    process_weather,
)


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_process_reviews(spark):
    df_reviews = spark.createDataFrame(
        [
            (1, 101, "Amsterdam", "2021-01-01", "great stay", "Alice"),
            (2, 102, "Berlin", "2021-01-02", "bad experience", "Bob"),
        ],
        ["id", "listing_id", "city", "date", "comments", "reviewer_name"],
    )
    df_listings = spark.createDataFrame([(101, 5), (102, 6)], ["listing_id", "host_id"])
    df_result = process_reviews(df_reviews, df_listings).orderBy("id").collect()
    assert df_result[0].host_id == 5
    assert df_result[1].sentiment == "negative"


def test_process_reviewers(spark):
    df_reviews = spark.createDataFrame(
        [
            (1, "Alice", "en", "2021-01-01"),
            (1, "Alice", "fr", "2021-01-02"),
        ],
        ["reviewer_id", "reviewer_name", "comment_language", "date"],
    )
    df_reviewers = process_reviewers(df_reviews).collect()[0]
    assert df_reviewers.reviewer_id == 1
    assert "en" in df_reviewers.languages_spoken


def test_process_listings_hosts(spark):
    df_global = spark.createDataFrame(
        [(1, "2021-01-01", 5, "Host1")],
        ["id", "last_scraped", "host_id", "host_name"],
    )
    df_monthly = spark.createDataFrame(
        [
            (1, "2021-02-01", 5, "Host1-new"),
            (2, "2021-02-01", 6, "Host2"),
        ],
        ["id", "last_scraped", "host_id", "host_name"],
    )
    listings, hosts = process_listings_hosts(df_global, df_monthly)
    assert listings.count() == 2
    host = hosts.filter("host_id = 5").collect()[0]
    assert host.host_name == "Host1-new"


def test_process_weather(spark):
    df_temp = spark.createDataFrame(
        [(1, "20210102", 20)], ["STAID", "DATE", "TG"]
    )
    df_rain = spark.createDataFrame(
        [(1, "20210102", 5)], ["STAID", "DATE", "RR"]
    )
    df_stations = spark.createDataFrame([(1, "Amsterdam")], ["STAID", "city"])
    df_weather = process_weather(df_temp, df_rain, df_stations).collect()[0]
    assert df_weather.city == "Amsterdam"
    assert abs(df_weather.temperature - 2.0) < 0.001
