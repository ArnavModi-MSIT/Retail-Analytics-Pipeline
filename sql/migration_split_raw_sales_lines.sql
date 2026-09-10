-- Run once against an existing database that already has raw_sales_lines
-- (from the earlier ELT pass). Splits the single harmonized landing table
-- into two source-native raw tables — see sql/ddl.sql for why.
-- New fresh installs get these tables directly from ddl.sql.

-- CASCADE: the old dbt staging view + marts (built on top of raw_sales_lines)
-- get dropped along with it. Expected — dbt rebuilds all of that from the
-- new raw tables on the next `dbt run`.
DROP TABLE IF EXISTS raw_sales_lines CASCADE;

CREATE TABLE raw_csv_sales (
    invoice         VARCHAR(20),
    stock_code      VARCHAR(20),
    description     VARCHAR(255),
    quantity        INTEGER,
    invoice_date    TIMESTAMP,
    price           NUMERIC(10, 2),
    customer_id     INTEGER,
    country         VARCHAR(100),
    loaded_at       TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE raw_api_carts (
    cart_id             INTEGER,
    user_id             INTEGER,
    product_id          INTEGER,
    title               VARCHAR(255),
    price               NUMERIC(10, 2),
    quantity            INTEGER,
    discount_percentage NUMERIC(5, 2),
    discounted_total    NUMERIC(10, 2),
    fetched_at          TIMESTAMP,
    loaded_at           TIMESTAMP NOT NULL DEFAULT now()
);
