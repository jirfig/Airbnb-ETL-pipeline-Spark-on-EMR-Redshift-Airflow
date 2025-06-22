#!/usr/bin/env python3

import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

from apps.utils import DEFAULT_BUCKET_NAME, build_paths

BUCKET_NAME = DEFAULT_BUCKET_NAME


def build_reviewers_table(df_reviews: DataFrame) -> DataFrame:
    """Aggregate reviewer information from reviews."""
    windowSpec = (
        Window.partitionBy("reviewer_id")
        .orderBy("date")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    df_reviewers = (
        df_reviews.withColumn("languages_spoken", F.collect_set("comment_language").over(windowSpec))
        .withColumn("latest", F.last("date").over(windowSpec))
        .filter("date == latest")
        .dropDuplicates(["reviewer_id"])
        .select("reviewer_id", "reviewer_name", "languages_spoken", "date")
        .withColumnRenamed("date", "last_updated")
    )
    return df_reviewers.withColumn("languages_spoken", F.array_join("languages_spoken", ","))


def main(base_uri: str):
    spark = SparkSession.builder.appName("process_reviewers").getOrCreate()

    sc = spark.sparkContext

    ## Paths
    TEST = False

    paths = build_paths(base_uri, scrape_year_month, TEST)

    df_reviews = spark.read.csv(
        paths.dim_model_reviews_new,
        header="True",
        inferSchema="True",
        multiLine="True",
        escape='"',
        ignoreLeadingWhiteSpace="True",
    )

    df_reviewers = build_reviewers_table(df_reviews)

    df_reviewers.write.csv(paths.dim_model_reviewers_new, escape='"', header="true")


if __name__ == "__main__":
    scrape_year_month = str(sys.argv[1])
    base_uri = sys.argv[2] if len(sys.argv) > 2 else f"s3://{BUCKET_NAME}"
    main(base_uri)
