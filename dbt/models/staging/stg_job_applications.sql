-- Flatten raw.job_applications (sources: greenhouse, airtable).
-- Parse stage timestamps and compute days spent moving to the current stage.

with source as (

    select
        source as source_system,
        source_id,
        payload
    from {{ source('raw', 'job_applications') }}
    where source in ('greenhouse', 'airtable')

)

select
    payload ->> 'source_id'                                  as application_id,
    source_system,
    payload ->> 'candidate_id'                               as candidate_id,
    payload ->> 'job_id'                                     as job_id,
    payload ->> 'job_title'                                  as job_title,
    payload ->> 'department'                                 as department,
    payload ->> 'stage'                                      as stage,
    payload ->> 'recruiter_id'                               as recruiter_id,
    (payload ->> 'applied_at')::timestamp                    as applied_at,
    (payload ->> 'stage_changed_at')::timestamp              as stage_changed_at,
    extract(
        day from (payload ->> 'stage_changed_at')::timestamp
                 - (payload ->> 'applied_at')::timestamp
    )                                                        as time_in_stage_days
from source
