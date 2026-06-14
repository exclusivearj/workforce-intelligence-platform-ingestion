-- Flatten the JSONB payload from raw.employees into typed columns.
-- Sources: workday + airtable (greenhouse provides applications, not employees).
-- Terminated employees whose termination_date is > 90 days in the past are dropped.

with source as (

    select
        source as source_system,
        source_id,
        payload,
        ingested_at
    from {{ source('raw', 'employees') }}
    where source in ('workday', 'airtable')

),

flattened as (

    select
        payload ->> 'source_id'                              as employee_id,
        source_system,
        payload ->> 'first_name'                             as first_name,
        payload ->> 'last_name'                              as last_name,
        payload ->> 'email'                                  as email,
        payload ->> 'department'                             as department,
        payload ->> 'job_title'                              as job_title,
        payload ->> 'level'                                  as level,
        payload ->> 'employment_type'                        as employment_type,
        payload ->> 'location'                               as location,
        payload ->> 'manager_id'                             as manager_id,
        (payload ->> 'hire_date')::date                      as hire_date,
        nullif(payload ->> 'termination_date', '')::date     as termination_date,
        nullif(payload ->> 'salary', '')::numeric            as salary,
        nullif(payload ->> 'performance_rating', '')         as performance_rating,
        ingested_at
    from source

)

select
    *,
    (termination_date is null or termination_date > current_date) as is_active
from flattened
where termination_date is null
   or termination_date > (current_date - interval '90 days')
