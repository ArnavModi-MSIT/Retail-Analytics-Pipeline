-- Ports transform/schema_validation.py's classify_rows() (deleted — this IS
-- that logic now, as SQL). Same precedence as the original: return check
-- first (an invoice starting with 'C' is ALWAYS a return, regardless of
-- price/nulls), quarantine only evaluated on non-returns, everything else valid.
-- Nothing is dropped — every row keeps row_status so it stays queryable.

select
    *,
    invoice like 'C%' as is_return,
    case
        when invoice like 'C%' then 'return'
        when price < 0
            or customer_id is null
            or description is null
            or invoice_date is null
            then 'quarantine'
        else 'valid'
    end as row_status

from {{ ref('int_sales_unioned') }}
