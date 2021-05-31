
#!/usr/bin/env python3

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from sparknlp.pretrained import PretrainedPipeline
from sparknlp.base import *
from sparknlp.annotator import *
import sys
import boto3

def main():
    spark = SparkSession \
        .builder \
        .appName("process_reviews") \
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

    df_reviews_monthly = spark.read.parquet(path_out_city_reviews_data)

    df_listings = spark.read.csv(dim_model_listings_new,header="True", inferSchema="True",multiLine="True",escape='"',ignoreLeadingWhiteSpace="True")

    if not model_exists(dim_model_reviews):
        df_reviews_delta = df_reviews_monthly
        
    else:
        df_reviews = spark.read.csv(dim_model_reviews,header="True", inferSchema="True",multiLine="True",escape='"',ignoreLeadingWhiteSpace="True")
        df_reviews.createOrReplaceTempView("reviews")
        df_reviews_monthly.createOrReplaceTempView("reviews_monthly")

        query="""
        SELECT *
        FROM reviews_monthly
        WHERE reviews_monthly.date >= 
            (SELECT max(reviews.date)
             FROM reviews)   
        """
        df_reviews_delta = spark.sql(query)

    df_reviews_delta.createOrReplaceTempView("reviews_delta")
    df_listings.createOrReplaceTempView("listings")

    query="""
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
    #spark = sparknlp.start()
    language_detector = PretrainedPipeline('detect_language_220', lang='xx')
    df_result = language_detector.annotate(df_reviews_delta, column="comments")
    df_reviews_delta2 = df_result.withColumn("comment_language", F.concat_ws(",",F.col("language.result"))).drop("document").drop("sentence").drop("language").withColumnRenamed('text','comments')

    df_reviews_delta2.write.csv(dim_model_reviews_step2, escape='"', header="true")        

    df_reviews_delta2 = spark.read.csv(dim_model_reviews_step2,header="True", inferSchema="True",multiLine="True",escape='"',ignoreLeadingWhiteSpace="True")

    sentiment_analyzer = PretrainedPipeline('analyze_sentimentdl_use_imdb', lang='en')
    df_result_sentiment = sentiment_analyzer.annotate(df_reviews_delta2.filter(F.col("comment_language") == 'en'), column="comments")
    df_result_sentiment = df_result_sentiment.withColumn("sentiment", F.concat_ws(",",F.col("sentiment.result"))).drop("document").drop("sentence_embeddings").withColumnRenamed('text','comments')

    df_reviews_null = df_reviews_delta2.filter("comment_language is null").withColumn("sentiment", F.lit('n/a'))
    df_reviews_delta3 = df_reviews_delta2.filter("comment_language != 'en'").withColumn("sentiment", F.lit('n/a'))\
                        .union(df_result_sentiment)\
                        .union(df_reviews_null)

    if not model_exists(dim_model_reviews):
        df_reviews_delta3.write.csv(dim_model_reviews_new, escape='"', header="true")        
    else:
        df_reviews = spark.read.csv(dim_model_reviews,header="True", inferSchema="True",multiLine="True",escape='"',ignoreLeadingWhiteSpace="True")
        df_reviews_updated = df_reviews.union(df_reviews_delta3)
        # Its necessary to drop duplicates since some of the reviews submitted at the scrape date will be included twice
        df_reviews_updated = df_reviews_updated.dropDuplicates(["review_id"])
        df_reviews_updated.write.csv(dim_model_reviews_new, escape='"', header="true")          
     
   

if __name__ == "__main__":
    
    scrape_year_month = str(sys.argv[1])  
    main()

    
