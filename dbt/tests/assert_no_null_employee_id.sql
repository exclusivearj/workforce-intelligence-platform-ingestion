-- Singular test: dim_employees must never contain a null employee_id.
-- Returns offending rows; dbt fails the test if any are returned.
select employee_id
from {{ ref('dim_employees') }}
where employee_id is null
