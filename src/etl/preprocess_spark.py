from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


from langdetect import detect, DetectorFactory
DetectorFactory.seed = 20
raw_path = "drive/MyDrive/MachineLearning/data/raw/*.json.gz"
bronze_path = "drive/MyDrive/MachineLearning/data/bronze"
silver_path = "drive/MyDrive/MachineLearning/data/silver"
def detect_lang(text):
  try:
    return detect(text)
  except:
    return "unknown"

lang_udf = udf(detect_lang,StringType())


def main():
    # --------------------------
    # 1. Start Spark session
    # --------------------------
    spark = SparkSession.builder \
        .appName("AmazonSilverIngestion") \
        .getOrCreate()

    # --------------------------
    # 2. Read bronze data
    # --------------------------
    bronze_path = "data/bronze/reviews/"
    print("✅ Bronze schema:")
    df = spark.read.parquet(bronze_path)
    print("✅ Bronze schema:")
    df.printSchema()

    # --------------------------
    # 3. Select relevant columns
    # --------------------------
    cols = ["reviewerID", "asin", "reviewText", "summary", "overall", "reviewTime", "review_id", "ingested_at"]
    df = df.select([c for c in cols if c in df.columns])

    # --------------------------
    # 4. Create clean_text column
    # --------------------------
    def clean_text(text):
        import re
        if text is None:
            return ""
        text = text.lower().strip()
        text = re.sub(r"<[^>]*>", " ", text)  # remove HTML tags
        text = re.sub(r"\s+", " ", text)       # remove extra whitespace/newlines
        return text

    clean_text_udf = udf(clean_text, StringType())

    df = df.withColumn(
        "clean_text",
        clean_text_udf(F.concat_ws(" ", F.col("reviewText"), F.col("summary")))
    )

    # --------------------------
    # 5. Detect language
    # --------------------------
    df = df.withColumn("lang", lang_udf(F.col("clean_text")))

    # --------------------------
    # 6. Add ingestion timestamp
    # --------------------------
    df = df.withColumn("silver_ingested_at", F.current_timestamp())

    # --------------------------
    # 7. Write silver layer
    # --------------------------
    silver_path = "data/silver/reviews/"
    df.write.mode("overwrite").parquet(silver_path)
    print(f"✅ Silver data written to {silver_path}")

    # --------------------------
    # 8. Test read back
    # --------------------------
    df_silver = spark.read.parquet(silver_path)
    print(f"✅ Silver record count: {df_silver.count()}")
    df_silver.show(5, truncate=True)

    spark.stop()


if __name__ == "__main__":
    main()