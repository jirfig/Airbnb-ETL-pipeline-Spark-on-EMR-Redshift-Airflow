from pyspark.sql import SparkSession
from pathlib import Path

# Base directory
DATA_DIR = Path("data")
PREPROCESS_DIR = DATA_DIR / "input_parquets_airflow"

def count_parquet_rows():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("CountParquetRows") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        counts = {}
        
        # Global listings
        global_listings_path = PREPROCESS_DIR / "global_listings.parquet"
        if global_listings_path.exists():
            df = spark.read.parquet(str(global_listings_path))
            counts["global_listings"] = df.count()
            print(f"Global listings columns: {df.columns}")
        
        # City listings
        city_listings_path = PREPROCESS_DIR / "city_listings/2021-01/city_listings.parquet"
        if city_listings_path.exists():
            df = spark.read.parquet(str(city_listings_path))
            counts["city_listings"] = df.count()
            print(f"City listings columns: {df.columns}")
        
        # City reviews
        city_reviews_path = PREPROCESS_DIR / "city_reviews/2021-01/city_reviews.parquet"
        if city_reviews_path.exists():
            df = spark.read.parquet(str(city_reviews_path))
            counts["city_reviews"] = df.count()
            print(f"City reviews columns: {df.columns}")
        
        # City temperature
        temp_path = PREPROCESS_DIR / "city_temperature.parquet"
        if temp_path.exists():
            df = spark.read.parquet(str(temp_path))
            counts["city_temperature"] = df.count()
            print(f"City temperature columns: {df.columns}")
        
        # City rain
        rain_path = PREPROCESS_DIR / "city_rain.parquet"
        if rain_path.exists():
            df = spark.read.parquet(str(rain_path))
            counts["city_rain"] = df.count()
            print(f"City rain columns: {df.columns}")
        
        # Weather stations
        stations_path = PREPROCESS_DIR / "weather_stations.parquet"
        if stations_path.exists():
            df = spark.read.parquet(str(stations_path))
            counts["weather_stations"] = df.count()
            print(f"Weather stations columns: {df.columns}")
        
        print("\nRow counts:")
        for file_name, count in counts.items():
            print(f"{file_name}: {count} rows")
            
    finally:
        spark.stop()

if __name__ == "__main__":
    count_parquet_rows()
    