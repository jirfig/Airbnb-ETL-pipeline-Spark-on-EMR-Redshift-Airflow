#!/usr/bin/env python3

import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from apps.utils import DEFAULT_BUCKET_NAME, build_paths

BUCKET_NAME = DEFAULT_BUCKET_NAME


def join_weather(df_temp: DataFrame, df_rain: DataFrame, df_stations: DataFrame) -> DataFrame:
    """Join weather measurements with station information."""
    df = df_temp.join(df_rain, ["STAID", "DATE"]).join(df_stations, "STAID")
    df = df.withColumn("date", F.to_date("DATE", "yyyyMMdd"))
    df = df.where(F.col("date") > F.to_date(F.lit("20090101"), "yyyyMMdd"))
    df = df.select(
        F.concat_ws("_", F.col("city"), F.col("date")).alias("weather_id"),
        "date",
        (F.col("TG") / 10).alias("temperature"),
        (F.col("RR") / 10).alias("rain"),
        "city",
    )
    return df.orderBy("date")


def main(base_uri: str):
    spark = SparkSession.builder.appName("process_weather").getOrCreate()

    sc = spark.sparkContext

    ## Paths
    paths = build_paths(base_uri, scrape_year_month)

    df_temp = spark.read.parquet(paths.path_out_city_temperature_data)
    df_rain = spark.read.parquet(paths.path_out_city_rain_data)
    df_stations = spark.read.parquet(paths.path_out_weather_stations)

    df_weather = join_weather(df_temp, df_rain, df_stations)

    df_weather.write.csv(paths.dim_model_weather_new, escape='"', header="true")


if __name__ == "__main__":
    scrape_year_month = str(sys.argv[1])
    base_uri = sys.argv[2] if len(sys.argv) > 2 else f"s3://{BUCKET_NAME}"
    main(base_uri)
