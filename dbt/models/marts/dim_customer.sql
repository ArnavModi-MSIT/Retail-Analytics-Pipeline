select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
    customer_id
from {{ ref('int_sales_classified') }}
where row_status = 'valid'
group by customer_id
