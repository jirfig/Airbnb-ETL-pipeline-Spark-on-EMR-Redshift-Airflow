from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.hooks.emr_hook import EmrHook
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook

from operators.s3_to_redshift_operator import S3ToRedshiftTransfer_custom


import boto3
import logging


## Paths    
TEST = False    
scrape_year_month = '{{ execution_date.strftime("%Y-%m") }}'

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



EMR_CLUSTER_NAME='Capstone'


default_args = {
    'owner': 'Jiri',
    'start_date': datetime(2021, 1, 1),
    'end_date': datetime(2021, 3, 2),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,    
    'depends_on_past': False    
}

dag = DAG('Airbnb-etl',
          default_args=default_args,
          catchup = True,
          description='Airbnb ETL pipeline: Spark on EMR, Redshift & Airflow',
          schedule_interval='@monthly',
          max_active_runs=1
        )


def get_cluster(): 
    "Get EMR cluster id"   
    emr_hook = EmrHook('aws_default')
    cluster_id = emr_hook.get_cluster_id_by_name(EMR_CLUSTER_NAME, ['RUNNING', 'WAITING'])
    if cluster_id == None:
        raise ValueError(f"Cluster {EMR_CLUSTER_NAME} is not available")
    return cluster_id

def path_exists(*op_args):
    "Check if keys in S3 bucket exist"
    s3_hook = S3Hook('aws_default')
    for path in op_args:
        val = s3_hook.check_for_prefix(bucket_name, path.replace(f"s3://{bucket_name}/",""),"/")   
        if val == False:
            raise ValueError(f"Path {path} is empty") 

def update_dim_model():
    "Delete the dimensional model and move in new dimensional model from 'temporary' folder"
    s3_hook = S3Hook('aws_default')
    keys_new_model = s3_hook.list_keys(bucket_name, prefix=dim_model_folder_new)
    if keys_new_model is None:
        raise ValueError("There is no model to update")

    keys_old_model = s3_hook.list_keys(bucket_name, prefix=dim_model_folder+"/")
    if keys_old_model is not None:
        for key in keys_old_model:
            s3_hook.delete_objects(bucket_name, key)

    for key in keys_new_model:
        s3_hook.copy_object(key,
                    key.replace(dim_model_folder_new,dim_model_folder),
                    source_bucket_name=bucket_name,
                    dest_bucket_name=bucket_name)
    
    for key in keys_new_model:
        s3_hook.delete_objects(bucket_name, key)

def check_redshift_loaded():
    "Check that all tables of dimensional models contain records"
    redshift_hook = PostgresHook('redshift_default')
    for table in ['listings', 'reviews', 'reviewers', 'hosts', 'weather']:
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")    
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} returned no results")
        num_records = records[0][0]    
        if num_records < 1:
            raise ValueError(f"Data quality on table {table} check failed, no records in the table")
        logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")



start_operator = PythonOperator(
    task_id='begin_execution', 
    python_callable=get_cluster,
    dag=dag)

preprocess_data_submit = EmrAddStepsOperator(
    task_id='preprocess_data_submit',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='begin_execution', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=[
            {
                'Name': 'preprocess_data',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ["spark-submit","--master","yarn", \
                                            "--deploy-mode", "client", \
                                            "s3://airbnbprj-us/apps/preprocess_data.py",'{{ execution_date.strftime("%Y-%m") }}']
                                            
                },
            }
    ],   
    dag=dag,
    provide_context = True
)

preprocess_data_wait = EmrStepSensor(
    task_id='preprocess_data_wait',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='begin_execution', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='preprocess_data_submit', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

preprocess_data_check = PythonOperator(
    task_id='preprocess_data_check', 
    python_callable=path_exists, 
    op_args=[path_out_global_listings,
             path_out_city_listings_data,
             path_out_city_reviews_data,
             path_out_city_temperature_data,
             path_out_city_rain_data,
             path_out_weather_stations],      
    dag=dag    
)


process_listings_hosts_submit = EmrAddStepsOperator(
    task_id='process_listings_hosts_submit',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='begin_execution', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=[
            {
                'Name': 'process_listings_hosts',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ["spark-submit","--master","yarn", \
                                            "--deploy-mode", "client", \
                                            "s3://airbnbprj-us/apps/process_listings_hosts.py",'{{ execution_date.strftime("%Y-%m") }}']
                                            
                },
            }
    ],   
    dag=dag,
    provide_context = True
)

process_listings_hosts_wait = EmrStepSensor(
    task_id='process_listings_hosts_wait',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='begin_execution', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='process_listings_hosts_submit', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

listings_hosts_check = PythonOperator(
    task_id='listings_hosts_check', 
    python_callable=path_exists,  
    op_args=[dim_model_listings_new,
             dim_model_hosts_new],      
    dag=dag    
)

process_reviews_submit = EmrAddStepsOperator(
    task_id='process_reviews_submit',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='begin_execution', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=[
            {
                'Name': 'process_reviews_submit',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ["spark-submit","--master","yarn", \
                                            "--deploy-mode", "client", \
                                            "s3://airbnbprj-us/apps/process_reviews.py",'{{ execution_date.strftime("%Y-%m") }}']
                                            
                },
            }
    ],   
    dag=dag,
    provide_context = True
)

process_reviews_wait = EmrStepSensor(
    task_id='process_reviews_wait',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='begin_execution', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='process_reviews_submit', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

reviews_check = PythonOperator(
    task_id='reviews_check', 
    python_callable=path_exists,  
    op_args=[dim_model_reviews_new],      
    dag=dag    
)


process_reviewers_submit = EmrAddStepsOperator(
    task_id='process_reviewers_submit',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='begin_execution', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=[
            {
                'Name': 'process_reviewers_submit',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ["spark-submit","--master","yarn", \
                                            "--deploy-mode", "client", \
                                            "s3://airbnbprj-us/apps/process_reviewers.py",'{{ execution_date.strftime("%Y-%m") }}']
                                            
                },
            }
    ],   
    dag=dag,
    provide_context = True
)

process_reviewers_wait = EmrStepSensor(
    task_id='process_reviewers_wait',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='begin_execution', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='process_reviewers_submit', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

reviewers_check = PythonOperator(
    task_id='reviewers_check', 
    python_callable=path_exists,  
    op_args=[dim_model_reviewers_new],      
    dag=dag    
)


process_weather_submit = EmrAddStepsOperator(
    task_id='process_weather_submit',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='begin_execution', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=[
            {
                'Name': 'process_weather_submit',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ["spark-submit","--master","yarn", \
                                            "--deploy-mode", "client", \
                                            "s3://airbnbprj-us/apps/process_weather.py",'{{ execution_date.strftime("%Y-%m") }}']
                                            
                },
            }
    ],   
    dag=dag,
    provide_context = True
)

process_weather_wait = EmrStepSensor(
    task_id='process_weather_wait',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='begin_execution', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='process_weather_submit', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

weather_check = PythonOperator(
    task_id='weather_check', 
    python_callable=path_exists,  
    op_args=[dim_model_weather_new],      
    dag=dag    
)

update_dim_model = PythonOperator(
    task_id='update_dim_model', 
    python_callable=update_dim_model,
    dag=dag)


drop_tables = PostgresOperator(
    task_id='drop_tables', 
    postgres_conn_id="redshift_default",
    sql="""
    DROP TABLE IF EXISTS listings;
    DROP TABLE IF EXISTS reviews;
    DROP TABLE IF EXISTS hosts;
    DROP TABLE IF EXISTS weather;
    DROP TABLE IF EXISTS reviewers;
    """,
    dag=dag)


create_tables = PostgresOperator(
    task_id='create_tables', 
    postgres_conn_id="redshift_default",
    sql="""
    CREATE TABLE listings(
    accommodates  integer,
    amenities  varchar(max),
    availability_30  varchar,
    availability_365  varchar,
    availability_60  varchar,
    availability_90  varchar,
    bathrooms  varchar,
    bathrooms_text  varchar,
    bedrooms  integer,
    beds  integer,
    calculated_host_listings_count  integer,
    calculated_host_listings_count_entire_homes  integer,
    calculated_host_listings_count_private_rooms  integer,
    calculated_host_listings_count_shared_rooms  integer,
    calendar_last_scraped  varchar,
    calendar_updated  varchar,
    city  varchar(max),
    description  varchar(max),
    first_review  date,
    has_availability  varchar,
    host_id  integer,
    listing_id  varchar,
    instant_bookable  varchar,
    last_review  date,
    last_scraped  date,
    latitude  float,
    license  varchar(max),
    listing_url  varchar,
    longitude  float,
    maximum_maximum_nights  integer,
    maximum_minimum_nights  integer,
    maximum_nights  integer,
    maximum_nights_avg_ntm  float,
    minimum_maximum_nights  integer,
    minimum_minimum_nights  integer,
    minimum_nights  integer,
    minimum_nights_avg_ntm  float,
    name  varchar(max),
    neighborhood_overview  varchar(max),
    neighbourhood  varchar,
    neighbourhood_cleansed  varchar,
    neighbourhood_group_cleansed  varchar,
    number_of_reviews  integer,
    number_of_reviews_l30d  integer,
    number_of_reviews_ltm  integer,
    picture_url  varchar,
    price  varchar,
    property_type  varchar,
    review_scores_accuracy  integer,
    review_scores_checkin  integer,
    review_scores_cleanliness  integer,
    review_scores_communication  integer,
    review_scores_location  integer,
    review_scores_rating  integer,
    review_scores_value  integer,
    reviews_per_month  float,
    room_type  varchar,
    scrape_id  varchar,
    scrape_month  integer,
    scrape_year  integer
    );

    CREATE TABLE weather(
    weather_id varchar,
    date date,
    temperature float,
    rain float,
    city varchar
    );

    CREATE TABLE reviews(
    review_id integer,
    reviewer_id integer,
    listing_id integer,
    host_id integer,
    weather_id varchar,
    date date,
    reviewer_name varchar,
    comments varchar(max),
    comment_language varchar,
    sentiment varchar
    );

    CREATE TABLE hosts(
    host_id  integer,
    host_name  varchar,
    host_url  varchar,
    host_since  varchar,
    host_location  varchar(max),
    host_about  varchar(max),
    host_response_time  varchar,
    host_response_rate  varchar,
    host_acceptance_rate  varchar,
    host_is_superhost  varchar,
    host_thumbnail_url  varchar,
    host_picture_url  varchar,
    host_neighbourhood  varchar,
    host_listings_count  integer,
    host_total_listings_count  integer,
    host_verifications  varchar,
    host_has_profile_pic  varchar,
    host_identity_verified  varchar,
    last_scraped  varchar
    );
    
    CREATE TABLE reviewers(
    reviewer_id integer, 
    reviewer_name varchar, 
    languages_spoken varchar,
    last_updated date
    );
    """,
    dag=dag
) 


copy_listings_to_redshift = S3ToRedshiftTransfer_custom(
    table='listings',    
    s3_key=dim_model_listings,
    task_id='copy_listings_to_redshift',  
    copy_options=('CSV','IGNOREHEADER 1'),
    dag=dag    
)

copy_hosts_to_redshift = S3ToRedshiftTransfer_custom(
    table='hosts',    
    s3_key=dim_model_hosts,
    task_id='copy_hosts_to_redshift',  
    copy_options=('CSV','IGNOREHEADER 1'),
    dag=dag    
)

copy_reviews_to_redshift = S3ToRedshiftTransfer_custom(
    table='reviews',    
    s3_key=dim_model_reviews,
    task_id='copy_reviews_to_redshift',  
    copy_options=('CSV','IGNOREHEADER 1'),
    dag=dag    
)

copy_reviewers_to_redshift = S3ToRedshiftTransfer_custom(
    table='reviewers',    
    s3_key=dim_model_reviewers,
    task_id='copy_reviewers_to_redshift',  
    copy_options=('CSV','IGNOREHEADER 1'),
    dag=dag    
)

copy_weather_to_redshift = S3ToRedshiftTransfer_custom(
    table='weather',    
    s3_key=dim_model_weather,
    task_id='copy_weather_to_redshift',  
    copy_options=('CSV','IGNOREHEADER 1'),
    dag=dag    
)

redshift_check = PythonOperator(
    task_id='redshift_check', 
    python_callable=check_redshift_loaded,
    dag=dag)



end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# DAG
start_operator >> preprocess_data_submit >> preprocess_data_wait >> preprocess_data_check

preprocess_data_check >> process_listings_hosts_submit >> process_listings_hosts_wait >> listings_hosts_check 
listings_hosts_check >> process_reviews_submit >> process_reviews_wait >> reviews_check
reviews_check >> process_reviewers_submit >> process_reviewers_wait >> reviewers_check >> update_dim_model
preprocess_data_check >> process_weather_submit >> process_weather_wait >> weather_check >> update_dim_model 

update_dim_model >> drop_tables >> create_tables 

create_tables >> copy_listings_to_redshift >> redshift_check
create_tables >> copy_hosts_to_redshift >> redshift_check
create_tables >> copy_reviews_to_redshift >> redshift_check
create_tables >> copy_reviewers_to_redshift >> redshift_check
create_tables >> copy_weather_to_redshift >> redshift_check

redshift_check >> end_operator