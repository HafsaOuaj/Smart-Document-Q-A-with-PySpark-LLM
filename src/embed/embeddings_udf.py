from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
import tiktoken 
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import Chroma
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from langdetect import detect, DetectorFactory


def main():
    DetectorFactory.seed = 20
    silver_path = "../data/silver/silver.parquet"

    # ----------------------
    # 1️⃣ Chunking UDF
    # ----------------------
    def chunk_text(text, chunk_size=500, overlap=50):
        if not text:
            return []
        words = text.split()
        chunks = []
        start = 0
        while start < len(words):
            end = start + chunk_size
            chunk = " ".join(words[start:end])
            chunks.append(chunk)
            start = end - overlap  # overlap for context continuity
            if start < 0:  # safety
                start = 0
        return chunks

    chunk_udf = F.udf(chunk_text, ArrayType(StringType()))

    # ----------------------
    # 2️⃣ Apply chunking to silver dataset
    # ----------------------
    spark = SparkSession.builder \
            .appName("AmazonSilverIngestion") \
            .getOrCreate()

    df_silver= spark.read.parquet(silver_path)
    df_chunks = df_silver.withColumn("chunks", chunk_udf(F.col("clean_text")))

    # explode chunks into separate rows
    df_chunks = df_chunks.withColumn("chunk", F.explode(F.col("chunks")))
    df_chunks = df_chunks.withColumn("chunk_id", F.concat_ws("_", F.col("review_id"), F.monotonically_increasing_id()))

    df_chunks = df_chunks.select("chunk_id", "review_id", "asin", "lang", "chunk")

    # ----------------------
    # 3️⃣ Convert Spark DF to Pandas for Chroma ingestion
    # ----------------------
    df_pd = df_chunks.toPandas()
    df_pd.to_parquet("../data/gold/gold.parquet")
    print("✅ Converted to Pandas DF with shape:", df_pd.shape)