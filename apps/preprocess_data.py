#!/usr/bin/env python3

import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from apps.utils import DEFAULT_BUCKET_NAME, build_paths, model_exists

BUCKET_NAME = DEFAULT_BUCKET_NAME


def clean_global_listings(df: DataFrame) -> DataFrame:
    """Clean the global listings dataframe."""
    df = df.toDF(*[c.replace(" ", "_").lower() for c in df.columns])
    columns_to_drop = [
        "xl_picture_url",
        "cancellation_policy",
        "access",
        "features",
        "zipcode",
        "country_code",
        "smart_location",
        "country",
        "security_deposit",
        "medium_url",
        "transit",
        "cleaning_fee",
        "street",
        "experiences_offered",
        "thumbnail_url",
        "extra_people",
        "weekly_price",
        "notes",
        "house_rules",
        "monthly_price",
        "summary",
        "square_feet",
        "interaction",
        "state",
        "jurisdiction_names",
        "market",
        "geolocation",
        "space",
        "bed_type",
        "guests_included",
    ]
    df = df.drop(*columns_to_drop)
    return df.withColumn("scrape_year", F.year(F.col("last_scraped"))).withColumn(
        "scrape_month", F.month(F.col("last_scraped"))
    )


def main(base_uri: str):
    spark = SparkSession.builder.appName("preprocess_raw_data").getOrCreate()

    sc = spark.sparkContext

    ## Paths
    TEST = False

    paths = build_paths(base_uri, scrape_year_month, TEST)

    if not model_exists(paths.path_out_global_listings):
        df_global_listings = spark.read.csv(
            paths.raw_global_listings,
            header="True",
            inferSchema="True",
            multiLine="True",
            escape='"',
            ignoreLeadingWhiteSpace="True",
            sep=";",
        )
        df_global_listings = clean_global_listings(df_global_listings)

        if TEST:
            df_global_listings.filter("city = 'Amsterdam'").write.partitionBy("scrape_year", "scrape_month").parquet(
                paths.path_out_global_listings
            )
        else:
            df_global_listings.write.partitionBy("scrape_year", "scrape_month").parquet(paths.path_out_global_listings)

    if not model_exists(paths.path_out_city_listings_data):
        df_city_listings = spark.read.csv(
            paths.raw_city_listings,
            header="True",
            inferSchema="True",
            multiLine="True",
            escape='"',
            ignoreLeadingWhiteSpace="True",
        )
        df_city_listings = df_city_listings.withColumn("city", F.element_at(F.split(F.input_file_name(), "/"), -3))
        df_city_listings = df_city_listings.withColumn("scrape_year", F.year(F.col("last_scraped"))).withColumn(
            "scrape_month", F.month(F.col("last_scraped"))
        )

        if TEST:
            df_city_listings.filter("city = 'Amsterdam'").write.partitionBy("scrape_year", "scrape_month").parquet(
                paths.path_out_city_listings_data
            )
        else:
            df_city_listings.write.partitionBy("scrape_year", "scrape_month").parquet(paths.path_out_city_listings_data)

    if not model_exists(paths.path_out_city_reviews_data):
        df_city_reviews = spark.read.csv(
            paths.raw_city_reviews,
            header="True",
            inferSchema="True",
            multiLine="True",
            escape='"',
            ignoreLeadingWhiteSpace="True",
        )
        df_city_reviews = df_city_reviews.withColumn("city", F.element_at(F.split(F.input_file_name(), "/"), -3))
        df_city_reviews = df_city_reviews.withColumn("year", F.year(F.col("date"))).withColumn(
            "month", F.month(F.col("date"))
        )

        if TEST:
            df_city_reviews.filter("city = 'Amsterdam'").write.partitionBy("year", "month", "city").parquet(
                paths.path_out_city_reviews_data
            )
        else:
            df_city_reviews.write.partitionBy("year", "month", "city").parquet(paths.path_out_city_reviews_data)

    if not model_exists(paths.path_out_city_temperature_data):
        text = (
            sc.textFile(paths.raw_city_temperature)
            .map(lambda line: line.replace(" ", "").split(","))
            .filter(lambda line: len(line) == 5)
            .filter(lambda line: line[0] != "STAID")
        )

        df = spark.createDataFrame(text)
        columns = ["STAID", "SOUID", "DATE", "TG", "Q_TG"]
        df = df.toDF(*columns)
        df.write.parquet(paths.path_out_city_temperature_data)

    if not model_exists(paths.path_out_city_rain_data):
        text = (
            sc.textFile(paths.raw_city_rain)
            .map(lambda line: line.replace(" ", "").split(","))
            .filter(lambda line: len(line) == 5)
            .filter(lambda line: line[0] != "STAID")
        )

        df = spark.createDataFrame(text)
        columns = ["STAID", "SOUID", "DATE", "RR", "Q_TG"]
        df = df.toDF(*columns)
        df.write.parquet(paths.path_out_city_rain_data)

    if not model_exists(paths.path_out_weather_stations):
        station_city = [(593, "Amsterdam"), (41, "Berlin"), (1860, "London"), (11249, "Paris")]
        columns = ["STAID", "city"]
        df_stations = spark.createDataFrame(data=station_city, schema=columns)
        df_stations.write.parquet(paths.path_out_weather_stations)


if __name__ == "__main__":
    scrape_year_month = str(sys.argv[1])
    base_uri = sys.argv[2] if len(sys.argv) > 2 else f"s3://{BUCKET_NAME}"
    main(base_uri)
