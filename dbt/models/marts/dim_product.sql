-- Ports transform/dim_product_dedup.py: picks the most frequent Description
-- per stock_code (mode), not first/last-loaded, since Description varies
-- for the same stock_code in this dataset (data quality issue).

with valid_sales as (

    select * from {{ ref('int_sales_classified') }}
    where row_status = 'valid'

),

description_counts as (

    select
        stock_code,
        description,
        count(*) as desc_count
    from valid_sales
    group by stock_code, description

),

ranked as (

    select
        *,
        row_number() over (
            partition by stock_code
            order by desc_count desc
        ) as rn
    from description_counts

)

select
    {{ dbt_utils.generate_surrogate_key(['stock_code']) }} as product_key,
    stock_code,
    description
from ranked
where rn = 1
