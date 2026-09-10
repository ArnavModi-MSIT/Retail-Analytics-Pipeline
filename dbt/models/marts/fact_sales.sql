-- Grain: one row per (invoice_no, stock_code) line item. Only row_status='valid'
-- rows reach the fact table — returns and quarantine stay queryable in
-- int_sales_classified but never make it into the star schema, matching
-- the original PySpark behavior.

with sales as (

    select
        invoice as invoice_no,
        stock_code,
        customer_id,
        country,
        price as unit_price,
        quantity,
        invoice_date,
        source,
        cast(to_char(invoice_date, 'YYYYMMDD') as integer) as date_key,
        round(quantity * price, 2) as revenue
    from {{ ref('int_sales_classified') }}
    where row_status = 'valid'

)

select
    sales.invoice_no,
    dim_product.product_key,
    dim_customer.customer_key,
    dim_date.date_key,
    dim_country.country_key,
    sales.invoice_date as invoice_datetime,
    sales.quantity,
    sales.unit_price,
    sales.revenue,
    sales.source

from sales
inner join {{ ref('dim_product') }}  as dim_product  on sales.stock_code  = dim_product.stock_code
inner join {{ ref('dim_customer') }} as dim_customer on sales.customer_id = dim_customer.customer_id
inner join {{ ref('dim_country') }}  as dim_country  on sales.country     = dim_country.country_name
inner join {{ ref('dim_date') }}     as dim_date     on sales.date_key    = dim_date.date_key
