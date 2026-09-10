from datetime import datetime

from csv_ingest import RAW_SCHEMA, cast_types


def make_df(spark, **overrides):
    row = {
        "Invoice": "536365",
        "StockCode": "85048",
        "Description": "CHRISTMAS GLASS BALL",
        "Quantity": 12,
        "InvoiceDate": "2010-09-21 10:47:00",
        "Price": 6.95,
        "Customer ID": 13085.0,
        "Country": "United Kingdom",
    }
    row.update(overrides)
    data = [tuple(row[f.name] for f in RAW_SCHEMA.fields)]
    return spark.createDataFrame(data, schema=RAW_SCHEMA)


def test_invoice_date_string_is_cast_to_timestamp(spark):
    df = make_df(spark)
    result = cast_types(df).collect()[0]

    assert result["InvoiceDate"] == datetime(2010, 9, 21, 10, 47)


def test_customer_id_float_is_cast_to_int_and_renamed(spark):
    df = make_df(spark)
    result = cast_types(df).collect()[0]

    assert result["CustomerID"] == 13085
    assert "Customer ID" not in result.asDict()


def test_no_rows_are_dropped_or_bucketed(spark):
    # cast_types does type-casting only — nulls, negative prices, etc. all
    # pass through untouched. Quality bucketing is dbt's job now.
    df = make_df(spark, Price=-11.62, **{"Customer ID": None})
    result = cast_types(df).collect()[0]

    assert result["Price"] == -11.62
    assert result["CustomerID"] is None
