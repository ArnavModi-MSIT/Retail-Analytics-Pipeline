-- Singular test at WARN severity, not ERROR: some quarantine rate is
-- expected and documented (README cites 22.7% on the historical CSV) — that
-- is not a failure. This test exists to catch a SPIKE, not the known
-- baseline, so it warns instead of failing the Airflow DAG on every run.

{{ config(severity='warn') }}

with stats as (

    select
        count(*) filter (where row_status = 'quarantine') as quarantine_count,
        count(*) filter (where row_status in ('valid', 'quarantine')) as non_return_count
    from {{ ref('int_sales_classified') }}

)

select *
from stats
where non_return_count > 0
  and quarantine_count::float / non_return_count > 0.30
