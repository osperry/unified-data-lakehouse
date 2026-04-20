{{ config(
    materialized='table',
    schema='gold'
) }}

select
    complaint_date,
    borough,
    count(*) as total_complaints,
    sum(case when status = 'CLOSED' then 1 else 0 end) as closed_complaints,
    sum(case when status in ('OPEN','IN PROGRESS','PENDING','ASSIGNED','STARTED') then 1 else 0 end) as open_complaints,
    round(avg(case when resolution_days >= 0 then resolution_days end), 1) as avg_resolution_days,
    sum(case when is_resolution_anomaly then 1 else 0 end) as anomaly_count
from {{ ref('stg_complaints') }}
group by complaint_date, borough
