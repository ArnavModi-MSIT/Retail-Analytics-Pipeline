-- Retail Analytics Data Pipeline — raw landing tables
--
-- Pure ELT: PySpark only extracts, type-casts, and lands — no business rules,
-- no quality bucketing, no cross-source harmonization. Each source lands in
-- its OWN native shape; forcing CSV and API into one shape was itself a
-- transform, so that no longer happens before landing either.
--
-- Everything downstream (staging, intermediate normalization/classification,
-- and the star schema itself) is built and owned by dbt — see dbt/models/.
-- No NOT NULL constraints on data columns here: rejecting bad rows at the DB
-- layer would just be validation hiding in a new place. dbt's tests are
-- where data quality gets reported now.

-- =========================
-- RAW LANDING: raw_csv_sales
-- One row per raw Online Retail II (UCI) CSV line, type-cast only.
-- =========================
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

-- =========================
-- RAW LANDING: raw_api_carts
-- One row per DummyJSON /carts line item, in DummyJSON's own shape —
-- no Invoice/StockCode synthesis, no Country default. That's dbt's job.
-- =========================
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
