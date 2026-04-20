{{ config(
    materialized='table',
    schema='silver'
) }}

with raw as (
    select *
    from read_json_auto(
        '/app/data/bronze/nyc311/nyc311_*.json',
        union_by_name=true,
        ignore_errors=true
    )
),

cleaned as (
    select
        cast(unique_key as varchar)               as complaint_id,
        cast(created_date as timestamp)            as created_date,
        cast(closed_date as timestamp)             as closed_date,
        upper(trim(complaint_type))                as complaint_type,
        trim(descriptor)                           as descriptor,
        upper(trim(agency))                       as agency,
        trim(agency_name)                          as agency_name,
        upper(trim(coalesce(borough, 'UNSPECIFIED'))) as borough,
        trim(incident_address)                     as incident_address,
        trim(incident_zip)                         as zip_code,
        trim(city)                                 as city,
        upper(trim(status))                        as status,
        trim(community_board)                      as community_board,
        trim(police_precinct)                      as police_precinct,
        upper(trim(open_data_channel_type))        as channel_type,
        trim(location_type)                        as location_type,
        trim(resolution_description)               as resolution_description,
        cast(resolution_action_updated_date as timestamp) as resolution_date,
        case
            when cast(latitude as double) between 40.4 and 40.95
            then cast(latitude as double)
        end                                        as latitude,
        case
            when cast(longitude as double) between -74.3 and -73.7
            then cast(longitude as double)
        end                                        as longitude,

        case
            when closed_date is not null
            then datediff('day', cast(created_date as timestamp), cast(closed_date as timestamp))
        end                                        as resolution_days,

        case
            when closed_date is not null
             and cast(closed_date as timestamp) < cast(created_date as timestamp)
            then true
            else false
        end                                        as is_resolution_anomaly,

        cast(created_date as date)                 as complaint_date

    from raw
    where unique_key is not null
      and created_date is not null
),

deduped as (
    select *,
        row_number() over (
            partition by complaint_id
            order by created_date desc
        ) as rn
    from cleaned
)

select * exclude (rn)
from deduped
where rn = 1
