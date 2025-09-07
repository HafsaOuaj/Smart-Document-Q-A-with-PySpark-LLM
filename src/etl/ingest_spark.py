from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    # --------------------------
    # 1. Start Spark session
    # --------------------------
    spark = SparkSession.builder \
        .appName("AmazonBronzeIngestion") \
        .getOrCreate()

    # --------------------------
    # 2. Read raw Amazon JSON files
    # --------------------------
    raw_path = "data/raw/*.json.gz"
    df = spark.read.json(raw_path)

    print("✅ Schema of raw data:")
    df.printSchema()

    # --------------------------
    # 3. Preserve all raw fields
    #    + add ingestion timestamp
    # --------------------------
    df_bronze = df.withColumn("ingested_at", F.current_timestamp())

    # --------------------------
    # 4. Write to bronze layer
    # --------------------------
    bronze_path = "data/bronze/reviews/"
    (
        df_bronze
        .write
        .mode("overwrite")  # overwrite for repeatability
        .parquet(bronze_path)
    )
    print(f"✅ Bronze data written to {bronze_path}")

    # --------------------------
    # 5. Test read back
    # --------------------------
    df_check = spark.read.parquet(bronze_path)
    print(f"✅ Bronze record count: {df_check.count()}")
    df_check.show(5, truncate=True)

    spark.stop()


if __name__ == "__main__":
    main()
