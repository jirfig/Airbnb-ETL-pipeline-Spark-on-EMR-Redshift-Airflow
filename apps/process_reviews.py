#!/usr/bin/env python3

import random
import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from apps.utils import DEFAULT_BUCKET_NAME, build_paths, model_exists

BUCKET_NAME = DEFAULT_BUCKET_NAME


class DummyPretrainedPipeline:
    def __init__(self, name, lang="en"):
        self.name = name
        self.lang = lang

    def transform(self, df):
        if self.name == "detect_language_220":
            # Randomly assign languages
            languages = ["en", "es", "fr", "de", "it", "pt", "nl", "ru", "zh", "ja"]
            return df.withColumn("language", F.struct(F.array([F.lit(random.choice(languages))]).alias("result")))
        elif self.name == "analyze_sentimentdl_use_imdb":
            # Randomly assign sentiment
            sentiments = ["pos", "neg"]
            return df.withColumn("sentiment", F.struct(F.array([F.lit(random.choice(sentiments))]).alias("result")))
        return df


def get_pipeline(name, lang="en", use_dummy=False):
    if use_dummy:
        return DummyPretrainedPipeline(name, lang)
    else:
        # Import SparkNLP modules only when needed
        from sparknlp.pretrained import PretrainedPipeline

        return PretrainedPipeline(name, lang)


def apply_language_and_sentiment(df: DataFrame, language_detector, sentiment_analyzer) -> DataFrame:
    """Detect language and sentiment of review comments."""
    df_result = language_detector.transform(df)
    df_lang = (
        df_result.withColumn("comment_language", F.concat_ws(",", F.col("language.result")))
        .drop("document")
        .drop("sentence")
        .drop("language")
        .withColumnRenamed("text", "comments")
    )

    df_result_sentiment = sentiment_analyzer.transform(df_lang.filter(F.col("comment_language") == "en"))
    df_result_sentiment = (
        df_result_sentiment.withColumn("sentiment", F.concat_ws(",", F.col("sentiment.result")))
        .drop("document")
        .drop("sentence_embeddings")
        .withColumnRenamed("text", "comments")
    )

    df_reviews_null = df_lang.filter("comment_language is null").withColumn("sentiment", F.lit("n/a"))
    df_final = (
        df_lang.filter("comment_language != 'en'")
        .withColumn("sentiment", F.lit("n/a"))
        .union(df_result_sentiment)
        .union(df_reviews_null)
    )
    return df_final


def main(base_uri: str, use_dummy_pipeline: bool = False):
    spark = SparkSession.builder.appName("process_reviews").getOrCreate()

    sc = spark.sparkContext

    ## Paths
    paths = build_paths(base_uri, scrape_year_month)

    df_reviews_monthly = spark.read.parquet(paths.path_out_city_reviews_data)

    df_listings = spark.read.csv(
        paths.dim_model_listings_new,
        header="True",
        inferSchema="True",
        multiLine="True",
        escape='"',
        ignoreLeadingWhiteSpace="True",
    )

    if not model_exists(paths.dim_model_reviews):
        df_reviews_delta = df_reviews_monthly

    else:
        df_reviews = spark.read.csv(
            paths.dim_model_reviews,
            header="True",
            inferSchema="True",
            multiLine="True",
            escape='"',
            ignoreLeadingWhiteSpace="True",
        )
        df_reviews.createOrReplaceTempView("reviews")
        df_reviews_monthly.createOrReplaceTempView("reviews_monthly")

        query = """
        SELECT *
        FROM reviews_monthly
        WHERE reviews_monthly.date >= 
            (SELECT max(reviews.date)
             FROM reviews)   
        """
        df_reviews_delta = spark.sql(query)

    df_reviews_delta.createOrReplaceTempView("reviews_delta")
    df_listings.createOrReplaceTempView("listings")

    query = """
    SELECT r.id as review_id, r.reviewer_id, r.listing_id, listings.host_id as host_id, concat_ws("_",r.city, r.date) as weather_id, r.date, r.reviewer_name, r.comments 
    FROM reviews_delta r
    LEFT JOIN listings
    ON r.listing_id == listings.listing_id
    """
    df_reviews_delta = spark.sql(query)

    df_reviews_delta.write.csv(paths.dim_model_reviews_step1, escape='"', header="true")

    # Detect language, translate, detect sentiment
    language_detector = get_pipeline("detect_language_220", lang="xx", use_dummy=use_dummy_pipeline)
    df_result = language_detector.transform(df_reviews_delta)
    df_reviews_delta2 = (
        df_result.withColumn("comment_language", F.concat_ws(",", F.col("language.result")))
        .drop("document")
        .drop("sentence")
        .drop("language")
        .withColumnRenamed("text", "comments")
    )

    df_reviews_delta2.write.csv(paths.dim_model_reviews_step2, escape='"', header="true")

    df_reviews_delta2 = spark.read.csv(
        paths.dim_model_reviews_step2,
        header="True",
        inferSchema="True",
        multiLine="True",
        escape='"',
        ignoreLeadingWhiteSpace="True",
    )

    sentiment_analyzer = get_pipeline("analyze_sentimentdl_use_imdb", lang="en", use_dummy=use_dummy_pipeline)
    df_reviews_delta3 = apply_language_and_sentiment(df_reviews_delta2, language_detector, sentiment_analyzer)

    if not model_exists(paths.dim_model_reviews):
        df_reviews_delta3.write.csv(paths.dim_model_reviews_new, escape='"', header="true")
    else:
        df_reviews = spark.read.csv(
            paths.dim_model_reviews,
            header="True",
            inferSchema="True",
            multiLine="True",
            escape='"',
            ignoreLeadingWhiteSpace="True",
        )
        df_reviews_updated = df_reviews.union(df_reviews_delta3)
        # Its necessary to drop duplicates since some of the reviews submitted at the scrape date will be included twice
        df_reviews_updated = df_reviews_updated.dropDuplicates(["review_id"])
        df_reviews_updated.write.csv(paths.dim_model_reviews_new, escape='"', header="true")


if __name__ == "__main__":
    scrape_year_month = str(sys.argv[1])
    base_uri = sys.argv[2] if len(sys.argv) > 2 else f"s3://{BUCKET_NAME}"
    use_dummy = len(sys.argv) > 3 and sys.argv[3].lower() == "dummy"
    main(base_uri, use_dummy_pipeline=use_dummy)
