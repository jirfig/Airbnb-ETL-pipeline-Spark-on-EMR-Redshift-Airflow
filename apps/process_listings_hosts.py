#!/usr/bin/env python3
import argparse

from pyspark.sql import SparkSession

from apps.transformations import process_listings_hosts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process listings and hosts")
    parser.add_argument("--global-listings", required=True, help="Path to global listings file")
    parser.add_argument("--monthly-listings", required=True, help="Path to monthly listings file")
    parser.add_argument("--listings-out", required=True, help="Destination path for listings table")
    parser.add_argument("--hosts-out", required=True, help="Destination path for hosts table")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("process_listings_hosts").getOrCreate()

    df_global = spark.read.csv(args.global_listings, header=True, inferSchema=True, multiLine=True)
    df_monthly = spark.read.csv(args.monthly_listings, header=True, inferSchema=True, multiLine=True)

    df_listings, df_hosts = process_listings_hosts(df_global, df_monthly)
    df_listings.write.csv(args.listings_out, header=True, mode="overwrite")
    df_hosts.write.csv(args.hosts_out, header=True, mode="overwrite")


if __name__ == "__main__":
    main()
