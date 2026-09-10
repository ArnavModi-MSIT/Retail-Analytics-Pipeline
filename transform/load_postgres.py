"""
Loads staged parquet (written by transform/csv_ingest.py and
transform/api_cast.py) into two raw landing tables — raw_csv_sales and
raw_api_carts. Each source keeps its own native shape; no harmonization,
no dedup, no business rules. Everything downstream is dbt's job.

Expects data/staged/csv_sales and data/staged/api_sales to already exist —
run csv_ingest and api_cast (Airflow tasks) or their underlying scripts
first. Truncate-and-load on every run.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

JDBC_URL = os.environ.get(
    "JDBC_URL", "jdbc:postgresql://localhost:5432/retail_analytics"
)
JDBC_PROPERTIES = {
    "user": os.environ.get("PG_USER", "postgres"),
    "password": os.environ["PG_PASSWORD"],
    "driver": "org.postgresql.Driver",
}


def write_table(df: DataFrame, table: str, mode: str = "append") -> None:
    df.write.jdbc(url=JDBC_URL, table=table, mode=mode, properties=JDBC_PROPERTIES)


def to_snake_case_columns(df: DataFrame) -> DataFrame:
    """Postgres folds unquoted identifiers to lowercase but doesn't split
    words — 'StockCode' would land as the column 'stockcode', not
    'stock_code'. Rename explicitly so the JDBC write matches raw_csv_sales."""
    return df.select(
        F.col("Invoice").alias("invoice"),
        F.col("StockCode").alias("stock_code"),
        F.col("Description").alias("description"),
        F.col("Quantity").alias("quantity"),
        F.col("InvoiceDate").alias("invoice_date"),
        F.col("Price").alias("price"),
        F.col("CustomerID").alias("customer_id"),
        F.col("Country").alias("country"),
    )


def truncate_tables():
    conn = psycopg2.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        dbname=os.environ.get("PG_DB", "retail_analytics"),
        user=JDBC_PROPERTIES["user"],
        password=JDBC_PROPERTIES["password"],
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE raw_csv_sales, raw_api_carts;")
    conn.close()


def main():
    truncate_tables()

    spark = (
        SparkSession.builder
        .appName("retail-load-postgres")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )

    csv_sales = to_snake_case_columns(spark.read.parquet("data/staged/csv_sales"))
    api_sales = spark.read.parquet("data/staged/api_sales")

    write_table(csv_sales, "raw_csv_sales")
    write_table(api_sales, "raw_api_carts")

    print(f"Loaded: raw_csv_sales={csv_sales.count()}, raw_api_carts={api_sales.count()}")


if __name__ == "__main__":
    main()
