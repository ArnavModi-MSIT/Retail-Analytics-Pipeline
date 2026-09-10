-- Replaces sql/populate_dim_date.sql — generated with dbt_utils.date_spine
-- instead of a hand-maintained SQL script. Range is static and deliberately
-- wide (covers the historical CSV plus several years of future API runs);
-- bump end_date manually if the pipeline is still running past it.

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2009-01-01' as date)",
        end_date="cast('2028-12-31' as date)"
    ) }}

)

select
    cast(to_char(date_day, 'YYYYMMDD') as integer) as date_key,
    date_day                                        as full_date,
    extract(year from date_day)::smallint            as year,
    extract(quarter from date_day)::smallint          as quarter,
    extract(month from date_day)::smallint            as month,
    to_char(date_day, 'FMMonth')                    as month_name,
    extract(day from date_day)::smallint              as day,
    extract(isodow from date_day)::smallint           as day_of_week,  -- 1=Monday..7=Sunday
    to_char(date_day, 'FMDay')                      as day_name,
    extract(isodow from date_day) in (6, 7)         as is_weekend
from spine
