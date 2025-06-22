#!/usr/bin/env python3

import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

from apps.utils import DEFAULT_BUCKET_NAME, build_paths, model_exists

BUCKET_NAME = DEFAULT_BUCKET_NAME


def drop_host_columns(df: DataFrame) -> DataFrame:
    """Remove host related columns from the listings dataframe."""
    columns_to_drop = [
        "host_name",
        "host_url",
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
    ]
    return df.drop(*columns_to_drop)


def main(base_uri: str):
    spark = SparkSession.builder.appName("process_listings_hosts").getOrCreate()

    sc = spark.sparkContext

    ## Paths
    TEST = False

    paths = build_paths(base_uri, scrape_year_month, TEST)

    if not model_exists(paths.dim_model_listings):
        # import global listings
        df_listings_global = spark.read.parquet(paths.path_out_global_listings)
        df_listings_global.createOrReplaceTempView("global")
        query = """
        SELECT *, cast(null as string) as bathrooms_text, cast(null as integer) as calculated_host_listings_count_entire_homes, cast(null as integer) as calculated_host_listings_count_private_rooms,
         cast(null as integer) as calculated_host_listings_count_shared_rooms, cast(null as string) as host_has_profile_pic, cast(null as string) as host_identity_verified, cast(null as string) as host_is_superhost,
         cast(null as string) as instant_bookable, cast(null as integer) as maximum_maximum_nights, cast(null as integer) as maximum_minimum_nights, cast(null as double) as maximum_nights_avg_ntm, cast(null as integer) as minimum_maximum_nights,
         cast(null as integer) as minimum_minimum_nights, cast(null as double) as minimum_nights_avg_ntm, cast(null as integer) as number_of_reviews_l30d, cast(null as integer) as number_of_reviews_ltm
        FROM global
        """
        df_listings_hosts = spark.sql(query)
        df_listings_hosts = df_listings_hosts.select(sorted(df_listings_hosts.columns))
        df_listings_hosts = df_listings_hosts.withColumnRenamed("id", "listing_id")

    if not model_exists(paths.dim_model_listings):
        # drop hosts columns from listings, except host_id
        df_listings = drop_host_columns(df_listings_hosts)

    if model_exists(paths.dim_model_listings):
        df_listings = spark.read.csv(
            paths.dim_model_listings,
            header="True",
            inferSchema="True",
            multiLine="True",
            escape='"',
            ignoreLeadingWhiteSpace="True",
        )

    df_listings_hosts_monthly = spark.read.parquet(paths.path_out_city_listings_data)
    df_listings_hosts_monthly = df_listings_hosts_monthly.select(sorted(df_listings_hosts_monthly.columns))

    # drop hosts columns from listings, except host_id
    df_listings_monthly = drop_host_columns(df_listings_hosts_monthly).withColumnRenamed("id", "listing_id")

    # merge global and local listings, drop duplicates by filtering by latest scrape date
    df_listings_updated = df_listings.union(df_listings_monthly)
    windowSpec = (
        Window.partitionBy("listing_id")
        .orderBy("last_scraped")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    df_listings_updated = (
        df_listings_updated.withColumn("latest", F.last("last_scraped").over(windowSpec))
        .filter("last_scraped == latest")
        .dropDuplicates(["listing_id"])
        .drop("latest")
    )

    df_listings_updated.write.csv(paths.dim_model_listings_new, escape='"', header="true")

    if not model_exists(paths.dim_model_listings):
        # create hosts table
        df_listings_hosts.createOrReplaceTempView("listings")
        query = """
        SELECT host_id, host_name, host_url, host_since, host_location, host_about, host_response_time, host_response_rate, host_acceptance_rate,
        host_is_superhost, host_thumbnail_url, host_picture_url, host_neighbourhood, host_listings_count,
        host_total_listings_count, host_verifications, host_has_profile_pic, host_identity_verified, last_scraped     
        FROM listings
        """
        df_hosts = spark.sql(query)

    if not model_exists(paths.dim_model_listings):
        windowSpec = (
            Window.partitionBy("host_id")
            .orderBy("last_scraped")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )
        df_hosts = (
            df_hosts.withColumn("latest", F.last("last_scraped").over(windowSpec))
            .filter("last_scraped == latest")
            .dropDuplicates(["host_id"])
            .drop("latest")
        )

    if model_exists(paths.dim_model_listings):
        df_hosts = spark.read.csv(
            paths.dim_model_hosts,
            header="True",
            inferSchema="True",
            multiLine="True",
            escape='"',
            ignoreLeadingWhiteSpace="True",
        )

    # create hosts table
    df_listings_hosts_monthly.createOrReplaceTempView("listings")
    query = """
    SELECT host_id, host_name, host_url, host_since, host_location, host_about, host_response_time, host_response_rate, host_acceptance_rate,
    host_is_superhost, host_thumbnail_url, host_picture_url, host_neighbourhood, host_listings_count,
    host_total_listings_count, host_verifications, host_has_profile_pic, host_identity_verified, last_scraped     
    FROM listings
    """
    df_hosts_monthly = spark.sql(query)

    df_hosts_updated = df_hosts.union(df_hosts_monthly)
    windowSpec = (
        Window.partitionBy("host_id")
        .orderBy("last_scraped")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    df_hosts_updated = (
        df_hosts_updated.withColumn("latest", F.last("last_scraped").over(windowSpec))
        .filter("last_scraped == latest")
        .dropDuplicates(["host_id"])
        .drop("latest")
    )

    df_hosts_updated.write.csv(paths.dim_model_hosts_new, escape='"', header="true")


if __name__ == "__main__":
    scrape_year_month = str(sys.argv[1])
    base_uri = sys.argv[2] if len(sys.argv) > 2 else f"s3://{BUCKET_NAME}"
    main(base_uri)
