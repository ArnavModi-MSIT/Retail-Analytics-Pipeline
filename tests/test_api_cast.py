from datetime import datetime

from pyspark.sql import Row

from api_cast import cast_types


def test_fetched_at_string_is_cast_to_timestamp(spark):
    rows = [
        Row(
            cart_id=1,
            user_id=42,
            product_id=101,
            title="Essence Mascara Lash Princess",
            price=9.99,
            quantity=2,
            discount_percentage=7.17,
            discounted_total=18.55,
            fetched_at="2026-07-14T10:00:00+00:00",
        )
    ]
    df = spark.createDataFrame(rows)
    result = cast_types(df).collect()[0]

    assert result["fetched_at"] == datetime(2026, 7, 14, 10, 0, 0)


def test_no_column_renaming_or_business_rules_applied(spark):
    # DummyJSON's own shape passes through untouched — no Invoice/StockCode
    # synthesis, no Country default. That's dbt's job now.
    rows = [
        Row(
            cart_id=1, user_id=42, product_id=101, title="Widget",
            price=9.99, quantity=2, discount_percentage=0.0,
            discounted_total=19.98, fetched_at="2026-07-14T10:00:00+00:00",
        )
    ]
    df = spark.createDataFrame(rows)
    result = cast_types(df).collect()[0].asDict()

    assert set(result.keys()) == {
        "cart_id", "user_id", "product_id", "title", "price",
        "quantity", "discount_percentage", "discounted_total", "fetched_at",
    }
