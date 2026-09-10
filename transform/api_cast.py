"""
API ingestion cast step — DummyJSON /carts daily feed.
Pure E+L: reads the raw flattened JSON (ingestion/api_ingest.py already did
the E), casts fetched_at to a real timestamp, writes every row in
DummyJSON's own shape. No Invoice/StockCode synthesis, no Country default
— that normalization is dbt's job now (see
dbt/models/intermediate/int_api_carts_normalized.sql).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def load_api_raw(spark, path: str) -> DataFrame:
    return spark.read.option("multiline", "true").json(path)


def cast_types(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("cart_id").cast("int"),
        F.col("user_id").cast("int"),
        F.col("product_id").cast("int"),
        F.col("title"),
        F.round(F.col("price").cast("double"), 2).alias("price"),
        F.col("quantity").cast("int"),
        F.col("discount_percentage").cast("double"),
        F.round(F.col("discounted_total").cast("double"), 2).alias("discounted_total"),
        F.to_timestamp("fetched_at").alias("fetched_at"),
    )


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("retail-api-cast").getOrCreate()

    raw = load_api_raw(spark, "data/raw/api/carts_*.json")
    typed = cast_types(raw)

    print(f"Read {typed.count()} rows from data/raw/api/")

    typed.write.mode("overwrite").parquet("data/staged/api_sales")
