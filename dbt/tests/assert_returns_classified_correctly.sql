-- Singular test: a business rule, not a generic column property.
-- Any invoice starting with 'C' must be classified as a return, full stop —
-- this is the one thing that should NEVER be overridden by a quarantine
-- condition (matches the original classify_rows() precedence exactly).
-- A singular test PASSES when it returns zero rows.

select *
from {{ ref('int_sales_classified') }}
where invoice like 'C%'
  and row_status != 'return'
