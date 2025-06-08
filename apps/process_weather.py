#!/usr/bin/env python3
import argparse

from pyspark.sql import SparkSession

from apps.transformations import process_weather


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process weather data")
    parser.add_argument("--temperature", required=True, help="Path to temperature parquet")
    parser.add_argument("--rain", required=True, help="Path to rain parquet")
    parser.add_argument("--stations", required=True, help="Path to weather stations parquet")
    parser.add_argument("--output", required=True, help="Destination path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("process_weather").getOrCreate()
    df_temp = spark.read.parquet(args.temperature)
    df_rain = spark.read.parquet(args.rain)
    df_stations = spark.read.parquet(args.stations)
    df_weather = process_weather(df_temp, df_rain, df_stations)
    df_weather.write.csv(args.output, header=True, mode="overwrite")


if __name__ == "__main__":
    main()
