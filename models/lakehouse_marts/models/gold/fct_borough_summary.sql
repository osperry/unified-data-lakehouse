{{ config(
    materialized='table',
    schema='gold'
) }}

with precinct_stats as (
    select
        police_precinct,
        borough,
        count(*) as total_complaints,
        sum(case when status != 'CLOSED' then 1 else 0 end) as open_complaints,
        round(sum(case when status != 'CLOSED' then 1 else 0 end) * 100.0 / count(*), 1) as open_rate_pct,
        round(avg(case when resolution_days >= 0 then resolution_days end), 1) as avg_resolution_days,
        sum(case when is_resolution_anomaly then 1 else 0 end) as anomaly_count,
        round(sum(case when is_resolution_anomaly then 1 else 0 end) * 100.0 / count(*), 2) as anomaly_rate_pct
    from {{ ref('stg_complaints') }}
    where police_precinct != 'Unspecified'
    group by police_precinct, borough
    having count(*) >= 1000
),

top_complaint as (
    select
        police_precinct,
        complaint_type,
        count(*) as type_cnt,
        row_number() over (partition by police_precinct order by count(*) desc) as rn
    from {{ ref('stg_complaints') }}
    where police_precinct != 'Unspecified'
    group by police_precinct, complaint_type
)

select
    row_number() over (order by p.open_rate_pct desc, p.avg_resolution_days desc) as priority_rank,
    p.police_precinct,
    p.borough,
    p.total_complaints,
    p.open_complaints,
    p.open_rate_pct,
    p.avg_resolution_days,
    p.anomaly_count,
    p.anomaly_rate_pct,
    t.complaint_type as top_complaint_type
from precinct_stats p
left join top_complaint t
    on p.police_precinct = t.police_precinct
    and t.rn = 1
order by priority_rank
