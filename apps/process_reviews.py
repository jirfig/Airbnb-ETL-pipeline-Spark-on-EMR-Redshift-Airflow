#!/usr/bin/env python3

import sys
import random

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from utils import join_path, model_exists

BUCKET_NAME = "airbnbprj-us"

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

def main(base_uri: str, use_dummy_pipeline: bool = False):
    spark = SparkSession.builder.appName("process_reviews").getOrCreate()

    sc = spark.sparkContext

    ## Paths
    TEST = False

    # S3
    path_global_listings = "airbnb-listings.csv"
    path_city_listings = f"cities/*/{scrape_year_month}/listings.csv"
    path_city_reviews = f"cities/*/{scrape_year_month}/reviews.csv"

    path_city_temperature = "weather/ECA_blend_tg/*.txt"
    path_city_rain = "weather/ECA_blend_rr/*.txt"

    raw_data_folder = "raw"
    # input_parquet_folder = "input_parquets_notebook"
    # dim_model_folder = "dim_model_notebook"
    # dim_model_folder_new = "dim_model_notebook_temp"
    input_parquet_folder = "input_parquets_airflow"
    dim_model_folder = "dim_model_airflow"
    dim_model_folder_new = "dim_model_airflow_temp"

    if TEST:
        input_parquet_folder += "_test"
        dim_model_folder += "_test"
        dim_model_folder_new += "_test"

    raw_global_listings_path = join_path(base_uri, raw_data_folder, path_global_listings)
    raw_city_listings_path = join_path(base_uri, raw_data_folder, path_city_listings)
    raw_city_reviews_path = join_path(base_uri, raw_data_folder, path_city_reviews)
    raw_city_temperature_path = join_path(base_uri, raw_data_folder, path_city_temperature)
    raw_city_rain_data_path = join_path(base_uri, raw_data_folder, path_city_rain)

    path_out_global_listings = join_path(base_uri, input_parquet_folder, "global_listings.parquet")
    path_out_city_listings_data = join_path(
        base_uri, input_parquet_folder, f"city_listings/{scrape_year_month}/city_listings.parquet"
    )
    path_out_city_reviews_data = join_path(
        base_uri, input_parquet_folder, f"city_reviews/{scrape_year_month}/city_reviews.parquet"
    )
    path_out_city_temperature_data = join_path(base_uri, input_parquet_folder, "city_temperature.parquet")
    path_out_city_rain_data = join_path(base_uri, input_parquet_folder, "city_rain.parquet")
    path_out_weather_stations = join_path(base_uri, input_parquet_folder, "weather_stations.parquet")

    dim_model_listings = join_path(base_uri, dim_model_folder, "listings.csv")
    dim_model_hosts = join_path(base_uri, dim_model_folder, "hosts.csv")
    dim_model_reviews = join_path(base_uri, dim_model_folder, "reviews.csv")
    dim_model_reviewers = join_path(base_uri, dim_model_folder, "reviewers.csv")
    dim_model_weather = join_path(base_uri, dim_model_folder, "weather.csv")

    dim_model_listings_new = join_path(base_uri, dim_model_folder_new, "listings.csv")
    dim_model_hosts_new = join_path(base_uri, dim_model_folder_new, "hosts.csv")
    dim_model_reviews_new = join_path(base_uri, dim_model_folder_new, "reviews.csv")
    dim_model_reviewers_new = join_path(base_uri, dim_model_folder_new, "reviewers.csv")
    dim_model_weather_new = join_path(base_uri, dim_model_folder_new, "weather.csv")

    dim_model_reviews_step1 = join_path(base_uri, dim_model_folder_new, "reviews_step1.csv")
    dim_model_reviews_step2 = join_path(base_uri, dim_model_folder_new, "reviews_step2.csv")
    ##

    df_reviews_monthly = spark.read.parquet(path_out_city_reviews_data)

    df_listings = spark.read.csv(
        dim_model_listings_new,
        header="True",
        inferSchema="True",
        multiLine="True",
        escape='"',
        ignoreLeadingWhiteSpace="True",
    )

    if not model_exists(dim_model_reviews):
        df_reviews_delta = df_reviews_monthly

    else:
        df_reviews = spark.read.csv(
            dim_model_reviews,
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

    df_reviews_delta.write.csv(dim_model_reviews_step1, escape='"', header="true")

    if TEST:
        df_reviews_delta = df_reviews_delta.limit(10000)

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

    df_reviews_delta2.write.csv(dim_model_reviews_step2, escape='"', header="true")

    df_reviews_delta2 = spark.read.csv(
        dim_model_reviews_step2,
        header="True",
        inferSchema="True",
        multiLine="True",
        escape='"',
        ignoreLeadingWhiteSpace="True",
    )

    sentiment_analyzer = get_pipeline("analyze_sentimentdl_use_imdb", lang="en", use_dummy=use_dummy_pipeline)
    df_result_sentiment = sentiment_analyzer.transform(df_reviews_delta2.filter(F.col("comment_language") == "en"))
    df_result_sentiment = (
        df_result_sentiment.withColumn("sentiment", F.concat_ws(",", F.col("sentiment.result")))
        .drop("document")
        .drop("sentence_embeddings")
        .withColumnRenamed("text", "comments")
    )

    df_reviews_null = df_reviews_delta2.filter("comment_language is null").withColumn("sentiment", F.lit("n/a"))
    df_reviews_delta3 = (
        df_reviews_delta2.filter("comment_language != 'en'")
        .withColumn("sentiment", F.lit("n/a"))
        .union(df_result_sentiment)
        .union(df_reviews_null)
    )

    if not model_exists(dim_model_reviews):
        df_reviews_delta3.write.csv(dim_model_reviews_new, escape='"', header="true")
    else:
        df_reviews = spark.read.csv(
            dim_model_reviews,
            header="True",
            inferSchema="True",
            multiLine="True",
            escape='"',
            ignoreLeadingWhiteSpace="True",
        )
        df_reviews_updated = df_reviews.union(df_reviews_delta3)
        # Its necessary to drop duplicates since some of the reviews submitted at the scrape date will be included twice
        df_reviews_updated = df_reviews_updated.dropDuplicates(["review_id"])
        df_reviews_updated.write.csv(dim_model_reviews_new, escape='"', header="true")


if __name__ == "__main__":
    scrape_year_month = str(sys.argv[1])
    base_uri = sys.argv[2] if len(sys.argv) > 2 else f"s3://{BUCKET_NAME}"
    use_dummy = len(sys.argv) > 3 and sys.argv[3].lower() == "dummy"
    main(base_uri, use_dummy_pipeline=use_dummy)
