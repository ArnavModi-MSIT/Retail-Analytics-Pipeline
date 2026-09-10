-- Light cleanup only: trimming, no filtering, no renaming (raw_csv_sales'
-- columns are already the target names). Every raw row passes through,
-- nulls and all — that's the point of landing raw.

select
    invoice,
    stock_code,
    trim(description) as description,
    quantity,
    invoice_date,
    price,
    customer_id,
    trim(country) as country

from {{ source('retail_warehouse', 'raw_csv_sales') }}
