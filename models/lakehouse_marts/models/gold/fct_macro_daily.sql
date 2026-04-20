{{ config(
    materialized='table',
    schema='gold'
) }}

select
    observation_date,
    max(case when series_id = 'UNRATE' then value end) as unemployment_rate,
    max(case when series_id = 'CPIAUCSL' then value end) as cpi,
    max(case when series_id = 'FEDFUNDS' then value end) as fed_funds_rate,
    max(case when series_id = 'GDP' then value end) as gdp
from {{ ref('stg_fred_observations') }}
group by observation_date
order by observation_date
