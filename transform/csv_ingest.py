"""
CSV ingestion — Online Retail II (UCI) historical bulk load.
Pure E+L: reads the raw CSV, casts to usable types, writes every row
untouched. No quality bucketing, no business rules — that's dbt's job now
(see dbt/models/intermediate/int_sales_classified.sql).
"""

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

RAW_SCHEMA = StructType([
    StructField("Invoice", StringType(), nullable=True),
    StructField("StockCode", StringType(), nullable=True),
    StructField("Description", StringType(), nullable=True),
    StructField("Quantity", IntegerType(), nullable=True),
    StructField("InvoiceDate", StringType(), nullable=True),  # cast explicitly, don't trust inference
    StructField("Price", DoubleType(), nullable=True),
    StructField("Customer ID", DoubleType(), nullable=True),  # float in source; cast to int after read
    StructField("Country", StringType(), nullable=True),
])


def load_raw(spark, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, schema=RAW_SCHEMA)


def cast_types(df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    return (
        df.withColumn("InvoiceDate", F.to_timestamp("InvoiceDate", "yyyy-MM-dd HH:mm:ss"))
          .withColumn("CustomerID", F.col("Customer ID").cast(IntegerType()))
          .drop("Customer ID")
    )


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("retail-csv-ingest").getOrCreate()

    raw = load_raw(spark, "data/raw/online_retail_ii.csv")
    typed = cast_types(raw)

    print(f"Read {typed.count()} rows from online_retail_ii.csv")

    typed.write.mode("overwrite").parquet("data/staged/csv_sales")
