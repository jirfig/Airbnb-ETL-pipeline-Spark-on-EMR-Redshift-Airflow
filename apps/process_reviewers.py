#!/usr/bin/env python3
import argparse

from pyspark.sql import SparkSession

from apps.transformations import process_reviewers


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build reviewers dimension table")
    parser.add_argument("--reviews", required=True, help="Path to reviews file")
    parser.add_argument("--output", required=True, help="Destination path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("process_reviewers").getOrCreate()
    df_reviews = spark.read.csv(args.reviews, header=True, inferSchema=True, multiLine=True)
    df_reviewers = process_reviewers(df_reviews)
    df_reviewers.write.csv(args.output, header=True, mode="overwrite")


if __name__ == "__main__":
    main()
