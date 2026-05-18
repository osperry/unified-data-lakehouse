{{ config(materialized='table') }}

with raw as (
    select * from read_json_auto('/app/data/bronze/*.json', filename=true)
),
exploded as (
    select
        regexp_extract(filename, '([A-Z]+)_\d', 1) as series_id,
        unnest(observations) as obs
    from raw
)
select
    series_id,
    cast(obs.date as date) as observation_date,
    try_cast(obs.value as double) as value,
    current_timestamp as loaded_at
from exploded
where try_cast(obs.value as double) is not null
