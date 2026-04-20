{{ config(
    materialized='table',
    schema='gold'
) }}

select
    agency,
    agency_name,
    complaint_type,
    borough,
    police_precinct,
    year(created_date) as complaint_year,
    count(*) as anomaly_count,
    round(avg(resolution_days), 1) as avg_days_early,
    min(resolution_days) as worst_case_days,
    count(distinct complaint_id) as unique_complaints
from {{ ref('stg_complaints') }}
where is_resolution_anomaly = true
group by
    agency, agency_name, complaint_type,
    borough, police_precinct, year(created_date)
