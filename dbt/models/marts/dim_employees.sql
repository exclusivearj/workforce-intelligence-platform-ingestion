-- SCD Type 1 employee dimension built from stg_employees.
-- NOTE: salary and performance_rating are intentionally EXCLUDED here. They exist
-- in raw but are surfaced only through governance-managed restricted views.

with employees as (

    select * from {{ ref('stg_employees') }}

),

deduped as (

    -- One row per employee_id; prefer the most recently ingested record.
    select
        *,
        row_number() over (
            partition by employee_id order by ingested_at desc
        ) as rn
    from employees

)

select
    employee_id,
    first_name || ' ' || last_name        as full_name,
    email,
    department,
    job_title,
    level,
    hire_date,
    termination_date,
    is_active,
    employment_type,
    manager_id,
    location,
    ingested_at                            as created_at,
    current_timestamp                      as updated_at
from deduped
where rn = 1
