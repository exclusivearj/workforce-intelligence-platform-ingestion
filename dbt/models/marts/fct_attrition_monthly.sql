-- Monthly attrition by department.
-- attrition_rate_pct = terminations in month / avg headcount in month * 100.
-- Voluntary vs involuntary is simulated deterministically from the employee_id
-- hash (no real reason code in synthetic data); documented in README.

with months as (

    select generate_series(
        date_trunc('month', current_date - interval '2 years'),
        date_trunc('month', current_date),
        interval '1 month'
    )::date as month_start

),

employees as (

    select
        employee_id,
        department,
        hire_date,
        termination_date,
        -- Deterministic voluntary/involuntary split for demo purposes.
        (('x' || substr(md5(employee_id), 1, 8))::bit(32)::int % 4 <> 0) as is_voluntary
    from {{ ref('dim_employees') }}

),

terminations as (

    select
        date_trunc('month', termination_date)::date as month_start,
        department,
        count(*) filter (where is_voluntary)        as voluntary_terminations,
        count(*) filter (where not is_voluntary)    as involuntary_terminations,
        count(*)                                     as total_terminations
    from employees
    where termination_date is not null
    group by 1, 2

),

headcount as (

    select
        date_trunc('month', date)::date as month_start,
        department,
        avg(headcount)                  as avg_headcount
    from {{ ref('fct_headcount_daily') }}
    group by 1, 2

),

spine as (

    select
        months.month_start,
        headcount.department,
        headcount.avg_headcount
    from months
    inner join headcount on headcount.month_start = months.month_start

),

combined as (

    select
        to_char(spine.month_start, 'YYYY-MM')                  as year_month,
        spine.month_start,
        spine.department,
        coalesce(terminations.voluntary_terminations, 0)       as voluntary_terminations,
        coalesce(terminations.involuntary_terminations, 0)     as involuntary_terminations,
        coalesce(terminations.total_terminations, 0)           as total_terminations,
        spine.avg_headcount,
        case
            when spine.avg_headcount > 0
                then round(
                    coalesce(terminations.total_terminations, 0)
                    / spine.avg_headcount * 100, 2
                )
            else 0
        end                                                    as attrition_rate_pct
    from spine
    left join terminations
        on terminations.month_start = spine.month_start
        and terminations.department = spine.department

)

select
    year_month,
    department,
    voluntary_terminations,
    involuntary_terminations,
    total_terminations,
    round(avg_headcount, 1) as avg_headcount,
    attrition_rate_pct,
    round(
        avg(attrition_rate_pct) over (
            partition by department
            order by month_start
            rows between 11 preceding and current row
        ), 2
    ) as rolling_12m_attrition_rate_pct
from combined
order by department, year_month
