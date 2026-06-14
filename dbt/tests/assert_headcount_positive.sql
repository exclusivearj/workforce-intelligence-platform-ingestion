-- Singular test: headcount can never be negative.
-- Returns offending rows; dbt fails the test if any are returned.
select date, department, level, headcount
from {{ ref('fct_headcount_daily') }}
where headcount < 0
