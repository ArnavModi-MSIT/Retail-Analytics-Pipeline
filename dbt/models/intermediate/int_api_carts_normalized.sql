-- Ports transform/normalize_api_source.py's normalization (deleted — this
-- IS that logic now, as SQL). Documented fabricated fields, same as before:
--   - No real Country data -> defaulted to "Unknown"
--   - No real Invoice number -> synthesized as "API-{cart_id}"
--   - No StockCode -> API's numeric product_id, prefixed "API-" to
--     guarantee no collision with real UK-source StockCodes

select
    concat('API-', cart_id)    as invoice,
    concat('API-', product_id) as stock_code,
    title                      as description,
    quantity,
    fetched_at                 as invoice_date,
    round(price, 2)            as price,
    user_id                    as customer_id,
    'Unknown'                  as country,
    'api'                      as source

from {{ ref('stg_api_carts') }}
