select
    {{ dbt_utils.generate_surrogate_key(['country']) }} as country_key,
    country as country_name
from {{ ref('int_sales_classified') }}
where row_status = 'valid'
group by country
