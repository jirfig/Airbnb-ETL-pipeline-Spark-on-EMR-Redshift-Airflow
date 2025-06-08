#!/usr/bin/env python3

import sys

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from utils import join_path

BUCKET_NAME = "airbnbprj-us"


def main(base_uri: str):
    spark = SparkSession.builder.appName("process_weather").getOrCreate()

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

    df_temp = spark.read.parquet(path_out_city_temperature_data)
    df_rain = spark.read.parquet(path_out_city_rain_data)
    df_stations = spark.read.parquet(path_out_weather_stations)

    df_temp.createOrReplaceTempView("temp")
    df_rain.createOrReplaceTempView("rain")
    df_stations.createOrReplaceTempView("stations")

    query = """
    SELECT null as weather_id,to_date(temp.DATE, "yyyyMMdd") as date, temp.TG/10 as temperature, rain.RR/10 as rain, stations.city
    FROM temp
    JOIN rain
    ON temp.DATE == rain.DATE
    AND temp.STAID == rain.STAID
    JOIN stations
    ON temp.STAID == stations.STAID
    WHERE to_date(temp.DATE, "yyyyMMdd") > to_date('20090101',"yyyyMMdd")
    ORDER BY date
    """
    df_weather = spark.sql(query)
    df_weather = df_weather.withColumn("weather_id", F.concat_ws("_", "city", "date"))

    df_weather.write.csv(dim_model_weather_new, escape='"', header="true")


if __name__ == "__main__":
    scrape_year_month = str(sys.argv[1])
    base_uri = sys.argv[2] if len(sys.argv) > 2 else f"s3://{BUCKET_NAME}"
    main(base_uri)
