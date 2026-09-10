-- Light cleanup only, DummyJSON's own shape preserved. No Invoice/StockCode
-- synthesis, no Country default here — that's int_api_carts_normalized's job.

select
    cart_id,
    user_id,
    product_id,
    trim(title) as title,
    price,
    quantity,
    discount_percentage,
    discounted_total,
    fetched_at

from {{ source('retail_warehouse', 'raw_api_carts') }}
