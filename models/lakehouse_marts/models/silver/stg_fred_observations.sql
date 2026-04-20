{{ config(
    materialized='table',
    schema='silver'
) }}

with raw as (
    select *
    from read_json_auto(
        '/app/data/bronze/fred/*.json',
        filename=true
    )
),

exploded as (
    select
        regexp_extract(filename, '([A-Z]+)_\d', 1) as series_id,
        filename as src_file,
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
qualify row_number() over (
    partition by series_id, cast(obs.date as date)
    order by src_file desc
) = 1
