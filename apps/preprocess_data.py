
#!/usr/bin/env python3

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys
import boto3

def main():
    spark = SparkSession \
        .builder \
        .appName("preprocess_raw_data") \
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

        
    if not model_exists(path_out_global_listings):
        df_global_listings = spark.read.csv(raw_global_listings_path, header="True", inferSchema="True",multiLine="True",escape='"',ignoreLeadingWhiteSpace="True", sep=";")
        df_global_listings = df_global_listings.toDF(*[column.replace(" ","_").lower() for column in df_global_listings.columns])
        columns_to_drop = ['xl_picture_url', 'cancellation_policy', 'access', 'features\r', 'zipcode', 'country_code', 'smart_location',\
                           'country', 'security_deposit', 'medium_url', 'transit', 'cleaning_fee', 'street', 'experiences_offered', \
                           'thumbnail_url', 'extra_people', 'weekly_price', 'notes', 'house_rules', 'monthly_price', \
                           'summary', 'square_feet', 'interaction', 'state','jurisdiction_names', 'market', 'geolocation', \
                           'space', 'bed_type', 'guests_included']
        df_global_listings = df_global_listings.drop(*columns_to_drop)
        df_global_listings = df_global_listings.withColumn('scrape_year', F.year(F.col('last_scraped'))).withColumn('scrape_month', F.month(F.col('last_scraped')))
    
        if TEST:    
            df_global_listings.filter("city = 'Amsterdam'").write.partitionBy('scrape_year','scrape_month').parquet(path_out_global_listings)
        else:    
            df_global_listings.write.partitionBy('scrape_year','scrape_month').parquet(path_out_global_listings)

    if not model_exists(path_out_city_listings_data):
        df_city_listings = spark.read.csv(raw_city_listings_path, header="True", inferSchema="True",multiLine="True",escape='"',ignoreLeadingWhiteSpace="True")
        df_city_listings = df_city_listings.withColumn("city",F.element_at(F.split(F.input_file_name(),"/"), -3))
        df_city_listings = df_city_listings.withColumn('scrape_year', F.year(F.col('last_scraped'))).withColumn('scrape_month', F.month(F.col('last_scraped')))

        if TEST:    
            df_city_listings.filter("city = 'Amsterdam'").write.partitionBy('scrape_year','scrape_month').parquet(path_out_city_listings_data)
        else:    
            df_city_listings.write.partitionBy('scrape_year','scrape_month').parquet(path_out_city_listings_data)

    if not model_exists(path_out_city_reviews_data):
        df_city_reviews = spark.read.csv(raw_city_reviews_path, header="True", inferSchema="True",multiLine="True",escape='"',ignoreLeadingWhiteSpace="True")
        df_city_reviews = df_city_reviews.withColumn("city",F.element_at(F.split(F.input_file_name(),"/"), -3))
        df_city_reviews = df_city_reviews.withColumn('year', F.year(F.col('date'))).withColumn('month', F.month(F.col('date')))
        
        if TEST:    
            df_city_reviews.filter("city = 'Amsterdam'").write.partitionBy('year','month','city').parquet(path_out_city_reviews_data)
        else:
            df_city_reviews.write.partitionBy('year','month','city').parquet(path_out_city_reviews_data)

    if not model_exists(path_out_city_temperature_data):
        text = sc.textFile(raw_city_temperature_path) \
            .map(lambda line: line.replace(" ","").split(",")) \
            .filter(lambda line: len(line)==5) \
            .filter(lambda line: line[0]!="STAID")

        df = spark.createDataFrame(text)  
        columns = ["STAID", "SOUID", "DATE", "TG", "Q_TG"]
        df = df.toDF(*columns)
        df.write.parquet(path_out_city_temperature_data)

    if not model_exists(path_out_city_rain_data):
        text = sc.textFile(raw_city_rain_data_path) \
            .map(lambda line: line.replace(" ","").split(",")) \
            .filter(lambda line: len(line)==5) \
            .filter(lambda line: line[0]!="STAID")

        df = spark.createDataFrame(text)  
        columns = ["STAID", "SOUID", "DATE", "RR", "Q_TG"]
        df = df.toDF(*columns)
        df.write.parquet(path_out_city_rain_data)

    if not model_exists(path_out_weather_stations):
        station_city = [(593,'Amsterdam'), (41,'Berlin'), (1860,'London'),(11249,'Paris')]
        columns = ["STAID","city"]
        df_stations = spark.createDataFrame(data=station_city, schema = columns)
        df_stations.write.parquet(path_out_weather_stations)

if __name__ == "__main__":
    
    scrape_year_month = str(sys.argv[1])  
    main()

    
