import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window


def process_reviews(df_reviews: DataFrame, df_listings: DataFrame) -> DataFrame:
    """Join reviews with listings and add simple language and sentiment columns."""
    df = df_reviews.join(df_listings.select("listing_id", "host_id"), on="listing_id", how="left")
    df = df.withColumn("weather_id", F.concat_ws("_", F.col("city"), F.col("date")))
    df = df.withColumn("comment_language", F.lit("en"))
    df = df.withColumn(
        "sentiment",
        F.when(F.col("comments").rlike("good|great|excellent"), F.lit("positive"))
        .when(F.col("comments").rlike("bad|terrible"), F.lit("negative"))
        .otherwise(F.lit("neutral")),
    )
    return df


def process_reviewers(df_reviews: DataFrame) -> DataFrame:
    """Aggregate reviewer info from reviews table."""
    window = (
        Window.partitionBy("reviewer_id")
        .orderBy("date")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    df = (
        df_reviews.withColumn("languages_spoken", F.collect_set("comment_language").over(window))
        .withColumn("latest", F.last("date").over(window))
    )
    df = (
        df.filter(F.col("date") == F.col("latest"))
        .dropDuplicates(["reviewer_id"])
        .select("reviewer_id", "reviewer_name", "languages_spoken", "date")
        .withColumnRenamed("date", "last_updated")
    )
    df = df.withColumn("languages_spoken", F.array_join("languages_spoken", ","))
    return df


# columns that belong to host information
HOST_COLUMNS = [
    "host_id",
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
    "last_scraped",
]


LISTING_DROP_COLUMNS = [c for c in HOST_COLUMNS if c not in ("host_id", "last_scraped")]


def process_listings_hosts(
    df_listings_global: DataFrame, df_listings_monthly: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Merge global and monthly listing data and return listings and hosts tables."""
    drop_cols_global = [c for c in LISTING_DROP_COLUMNS if c in df_listings_global.columns]
    drop_cols_monthly = [c for c in LISTING_DROP_COLUMNS if c in df_listings_monthly.columns]
    listings_global = df_listings_global.drop(*drop_cols_global).withColumnRenamed("id", "listing_id")
    listings_monthly = df_listings_monthly.drop(*drop_cols_monthly).withColumnRenamed("id", "listing_id")

    df_listings = listings_global.unionByName(listings_monthly)
    win = (
        Window.partitionBy("listing_id")
        .orderBy("last_scraped")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    df_listings = (
        df_listings.withColumn("latest", F.last("last_scraped").over(win))
        .filter(F.col("last_scraped") == F.col("latest"))
        .dropDuplicates(["listing_id"])
        .drop("latest")
    )

    hosts_global = df_listings_global.select(*[c for c in HOST_COLUMNS if c in df_listings_global.columns])
    hosts_monthly = df_listings_monthly.select(*[c for c in HOST_COLUMNS if c in df_listings_monthly.columns])
    df_hosts = hosts_global.unionByName(hosts_monthly)
    win_h = (
        Window.partitionBy("host_id")
        .orderBy("last_scraped")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    df_hosts = (
        df_hosts.withColumn("latest", F.last("last_scraped").over(win_h))
        .filter(F.col("last_scraped") == F.col("latest"))
        .dropDuplicates(["host_id"])
        .drop("latest")
    )

    return df_listings, df_hosts


def process_weather(
    df_temp: DataFrame, df_rain: DataFrame, df_stations: DataFrame
) -> DataFrame:
    """Combine temperature and rain data with station mapping."""
    df = df_temp.join(df_rain, ["STAID", "DATE"]).join(df_stations, "STAID")
    df = df.withColumn("date", F.to_date(F.col("DATE"), "yyyyMMdd"))
    df = df.withColumn("temperature", F.col("TG") / 10).withColumn("rain", F.col("RR") / 10)
    df = df.filter(F.col("date") > F.to_date(F.lit("20090101"), "yyyyMMdd"))
    df = df.withColumn("weather_id", F.concat_ws("_", F.col("city"), F.col("date")))
    return df.select("weather_id", "date", "temperature", "rain", "city")
