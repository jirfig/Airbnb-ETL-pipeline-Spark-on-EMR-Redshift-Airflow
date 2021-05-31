
#!/usr/bin/env python3

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import sys
import boto3

def main():
    spark = SparkSession \
        .builder \
        .appName("process_reviewers") \
        .getOrCreate()

    sc = spark.sparkContext

    def model_exists(path):
        "Check if model exists by listing keys on provided path"
        s3_client = boto3.client('s3')
        response = s3_client.list_objects(Bucket=bucket_name, MaxKeys=1, Prefix=path.replace(f"s3://{bucket_name}/",""))
        if 'Contents' not in response:
           return False
        else:
           return True
    
    ## Paths    
    TEST = False    

    # S3
    path_global_listings = 'airbnb-listings.csv'
    path_city_listings = f'cities/*/{scrape_year_month}/listings.csv'    
    path_city_reviews = f'cities/*/{scrape_year_month}/reviews.csv'

    path_city_temperature = "weather/ECA_blend_tg/*.txt"
    path_city_rain = "weather/ECA_blend_rr/*.txt"

    raw_data_folder = "raw"
    # input_parquet_folder = "input_parquets_notebook"
    # dim_model_folder = "dim_model_notebook"
    # dim_model_folder_new = "dim_model_notebook_temp"
    input_parquet_folder = "input_parquets_airflow"
    dim_model_folder = "dim_model_airflow"
    dim_model_folder_new = "dim_model_airflow_temp"
    s3_path = "s3://{}/{}/{}"

    bucket_name = "airbnbprj-us"

    if TEST:
        input_parquet_folder += "_test"
        dim_model_folder += "_test"
        dim_model_folder_new += "_test"

    raw_global_listings_path = s3_path.format(bucket_name, raw_data_folder, path_global_listings)
    raw_city_listings_path = s3_path.format(bucket_name, raw_data_folder, path_city_listings)
    raw_city_reviews_path = s3_path.format(bucket_name, raw_data_folder, path_city_reviews)
    raw_city_temperature_path = s3_path.format(bucket_name, raw_data_folder, path_city_temperature)
    raw_city_rain_data_path = s3_path.format(bucket_name, raw_data_folder, path_city_rain)

    path_out_global_listings = s3_path.format(bucket_name, input_parquet_folder, 'global_listings.parquet')
    path_out_city_listings_data = s3_path.format(bucket_name, input_parquet_folder, f'city_listings/{scrape_year_month}/city_listings.parquet')
    path_out_city_reviews_data = s3_path.format(bucket_name, input_parquet_folder, f'city_reviews/{scrape_year_month}/city_reviews.parquet')
    path_out_city_temperature_data = s3_path.format(bucket_name, input_parquet_folder, 'city_temperature.parquet')
    path_out_city_rain_data = s3_path.format(bucket_name, input_parquet_folder, 'city_rain.parquet')
    path_out_weather_stations = s3_path.format(bucket_name, input_parquet_folder, 'weather_stations.parquet')

    dim_model_listings = s3_path.format(bucket_name, dim_model_folder, 'listings.csv')
    dim_model_hosts = s3_path.format(bucket_name, dim_model_folder, 'hosts.csv')
    dim_model_reviews = s3_path.format(bucket_name, dim_model_folder, 'reviews.csv')
    dim_model_reviewers = s3_path.format(bucket_name, dim_model_folder, 'reviewers.csv')
    dim_model_weather = s3_path.format(bucket_name, dim_model_folder, 'weather.csv')

    dim_model_listings_new = s3_path.format(bucket_name, dim_model_folder_new, 'listings.csv')
    dim_model_hosts_new = s3_path.format(bucket_name, dim_model_folder_new, 'hosts.csv')
    dim_model_reviews_new = s3_path.format(bucket_name, dim_model_folder_new, 'reviews.csv')
    dim_model_reviewers_new = s3_path.format(bucket_name, dim_model_folder_new, 'reviewers.csv')
    dim_model_weather_new = s3_path.format(bucket_name, dim_model_folder_new, 'weather.csv')

    dim_model_reviews_step1 = s3_path.format(bucket_name, dim_model_folder_new, 'reviews_step1.csv')
    dim_model_reviews_step2 = s3_path.format(bucket_name, dim_model_folder_new, 'reviews_step2.csv')
    ##

    df_reviews = spark.read.csv(dim_model_reviews_new,header="True", inferSchema="True",multiLine="True",escape='"',ignoreLeadingWhiteSpace="True")

    windowSpec  = Window.partitionBy("reviewer_id").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)                                             
    df_reviewers = df_reviews \
                   .withColumn("languages_spoken", F.collect_set('comment_language').over(windowSpec))\
                   .withColumn("latest", F.last("date").over(windowSpec))\
                   .filter("date == latest")\
                   .dropDuplicates(["reviewer_id"])\
                   .select("reviewer_id","reviewer_name", "languages_spoken", "date")\
                   .withColumnRenamed("date","last_updated")
    df_reviewers = df_reviewers.withColumn("languages_spoken",F.array_join("languages_spoken",","))

    df_reviewers.write.csv(dim_model_reviewers_new, escape='"', header="true")     
         
   

if __name__ == "__main__":
    
    scrape_year_month = str(sys.argv[1])  
    main()

    
