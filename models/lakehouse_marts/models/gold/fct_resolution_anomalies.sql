{{ config(
    materialized='table',
    schema='gold'
) }}

select
    complaint_id,
    created_date,
    closed_date,
    resolution_days,
    complaint_type,
    descriptor,
    agency,
    agency_name,
    borough,
    police_precinct,
    incident_address,
    zip_code,
    channel_type,
    resolution_description,
    year(created_date) as complaint_year
from {{ ref('stg_complaints') }}
where is_resolution_anomaly = true
