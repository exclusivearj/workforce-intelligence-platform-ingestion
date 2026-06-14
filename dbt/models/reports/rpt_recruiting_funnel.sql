-- Recruiting funnel by job and month, derived from stg_job_applications.
-- Grain: one row per job_id per month (by applied_at month).

with apps as (

    select
        job_id,
        job_title,
        department,
        stage,
        candidate_id,
        applied_at,
        time_in_stage_days,
        to_char(date_trunc('month', applied_at), 'YYYY-MM') as year_month
    from {{ ref('stg_job_applications') }}

),

aggregated as (

    select
        job_id,
        year_month,
        max(job_title)  as job_title,
        max(department) as department,
        count(*) filter (where stage = 'applied')       as applied_count,
        count(*) filter (where stage = 'phone_screen')  as phone_screen_count,
        count(*) filter (where stage = 'interview')      as interview_count,
        count(*) filter (where stage = 'offer')          as offer_count,
        count(*) filter (where stage = 'hired')          as hired_count,
        avg(time_in_stage_days) filter (where stage = 'hired') as application_to_hire_days_avg
    from apps
    group by job_id, year_month

)

select
    job_id,
    year_month,
    job_title,
    department,
    applied_count,
    phone_screen_count,
    interview_count,
    offer_count,
    hired_count,
    round(coalesce(application_to_hire_days_avg, 0), 1) as application_to_hire_days_avg,
    case
        when offer_count > 0
            then round(hired_count::numeric / offer_count * 100, 2)
        else 0
    end as offer_acceptance_rate_pct
from aggregated
order by year_month, job_id
