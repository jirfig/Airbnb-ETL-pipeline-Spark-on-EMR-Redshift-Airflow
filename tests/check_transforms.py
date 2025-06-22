import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from apps.preprocess_data import clean_global_listings
from apps.process_listings_hosts import drop_host_columns
from apps.process_reviewers import build_reviewers_table
from apps.process_reviews import apply_language_and_sentiment
from apps.process_weather import join_weather


def main():
    spark = SparkSession.builder.appName("transform-tests").getOrCreate()
    try:
        # clean_global_listings
        df = spark.createDataFrame([(1, "2021-01-03", "foo")], ["id", "Last Scraped", "extra_people"])
        result = clean_global_listings(df)
        assert "extra_people" not in result.columns
        row = result.first()
        assert row.scrape_year == 2021 and row.scrape_month == 1

        # drop_host_columns
        df_hosts = spark.createDataFrame([(1, "bob", 2)], ["id", "host_name", "host_listings_count"])
        result = drop_host_columns(df_hosts)
        assert "host_name" not in result.columns
        assert "host_listings_count" not in result.columns

        # apply_language_and_sentiment
        class LangPipe:
            def transform(self, df):
                return df.withColumn("language", F.struct(F.array(F.lit("en")).alias("result")))

        class SentPipe:
            def transform(self, df):
                return df.withColumn("sentiment", F.struct(F.array(F.lit("pos")).alias("result")))

        df_reviews = spark.createDataFrame(
            [(1, "Alice", 1, "Great")], ["review_id", "reviewer_name", "listing_id", "comments"]
        )
        enriched = apply_language_and_sentiment(df_reviews, LangPipe(), SentPipe())
        row = enriched.first()
        assert row.comment_language == "en"
        assert row.sentiment == "pos"

        # build_reviewers_table
        df_rev = spark.createDataFrame(
            [
                (1, "Alice", "en", "2021-01-01"),
                (1, "Alice", "de", "2021-01-02"),
            ],
            ["reviewer_id", "reviewer_name", "comment_language", "date"],
        )
        reviewers = build_reviewers_table(df_rev)
        row = reviewers.first()
        assert row.last_updated == "2021-01-02"
        assert set(row.languages_spoken.split(",")) == {"en", "de"}

        # join_weather
        df_temp = spark.createDataFrame([("593", "100", "20210102", 50)], ["STAID", "SOUID", "DATE", "TG"])
        df_rain = spark.createDataFrame([("593", "20210102", 5)], ["STAID", "DATE", "RR"])
        df_stations = spark.createDataFrame([("593", "Amsterdam")], ["STAID", "city"])
        weather = join_weather(df_temp, df_rain, df_stations)
        row = weather.first()
        assert row.city == "Amsterdam"
        assert abs(row.temperature - 5.0) < 1e-6
        assert abs(row.rain - 0.5) < 1e-6
        print("OK")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
