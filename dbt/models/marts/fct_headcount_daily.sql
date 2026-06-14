-- Daily active headcount by department, level, and employment_type.
-- Spine: every day over the past 2 years. An employee counts on a given day if
-- they were hired on/before it and not yet terminated as of that day.

with spine as (

    select generate_series(
        (current_date - interval '2 years')::date,
        current_date,
        interval '1 day'
    )::date as date

),

employees as (

    select
        employee_id,
        department,
        level,
        employment_type,
        hire_date,
        termination_date
    from {{ ref('dim_employees') }}

),

joined as (

    select
        spine.date,
        employees.department,
        employees.level,
        employees.employment_type
    from spine
    inner join employees
        on employees.hire_date <= spine.date
        and (
            employees.termination_date is null
            or employees.termination_date > spine.date
        )

)

select
    date,
    department,
    level,
    employment_type,
    count(*) as headcount
from joined
group by date, department, level, employment_type
order by date, department, level
