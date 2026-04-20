{{ config(
    materialized='table',
    schema='gold'
) }}

with monthly_complaints as (
    select
        date_trunc('month', complaint_date) as month_date,
        count(*) as total_complaints,
        sum(case when is_resolution_anomaly then 1 else 0 end) as anomaly_count,
        count(distinct complaint_type) as complaint_type_count,
        round(avg(case when resolution_days >= 0 then resolution_days end), 1) as avg_resolution_days
    from {{ ref('stg_complaints') }}
    group by date_trunc('month', complaint_date)
),

monthly_macro as (
    select
        date_trunc('month', observation_date) as month_date,
        avg(case when series_id = 'UNRATE' then value end) as unemployment_rate,
        avg(case when series_id = 'CPIAUCSL' then value end) as cpi,
        avg(case when series_id = 'FEDFUNDS' then value end) as fed_funds_rate,
        avg(case when series_id = 'GDP' then value end) as gdp
    from {{ ref('stg_fred_observations') }}
    group by date_trunc('month', observation_date)
)

select
    c.month_date,
    c.total_complaints,
    c.anomaly_count,
    c.complaint_type_count,
    c.avg_resolution_days,
    m.unemployment_rate,
    m.cpi,
    m.fed_funds_rate,
    m.gdp
from monthly_complaints c
left join monthly_macro m
    on c.month_date = m.month_date
order by c.month_date
