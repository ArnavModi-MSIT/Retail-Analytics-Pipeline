-- Ports the union that used to happen in transform/load_postgres.py.
-- Both branches are already in the same shape by this point.

with csv_sales as (

    select
        invoice,
        stock_code,
        description,
        quantity,
        invoice_date,
        price,
        customer_id,
        country,
        'csv' as source
    from {{ ref('stg_csv_sales') }}

),

api_sales as (

    select * from {{ ref('int_api_carts_normalized') }}

)

select * from csv_sales
union all
select * from api_sales
