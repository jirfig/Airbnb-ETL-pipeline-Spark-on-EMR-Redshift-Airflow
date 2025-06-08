#!/usr/bin/env python3
import argparse

from pyspark.sql import SparkSession

from apps.transformations import process_reviews


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process Airbnb reviews")
    parser.add_argument("--reviews", required=True, help="Path to reviews file")
    parser.add_argument("--listings", required=True, help="Path to listings file")
    parser.add_argument("--output", required=True, help="Destination path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("process_reviews").getOrCreate()

    df_reviews = spark.read.csv(args.reviews, header=True, inferSchema=True, multiLine=True)
    df_listings = spark.read.csv(args.listings, header=True, inferSchema=True, multiLine=True)

    df_processed = process_reviews(df_reviews, df_listings)
    df_processed.write.csv(args.output, header=True, mode="overwrite")


if __name__ == "__main__":
    main()
