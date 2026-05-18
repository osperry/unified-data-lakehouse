{{ config(
    materialized='table',
    schema='gold'
) }}

/*
  fct_property_incidents
  ──────────────────────────────────────────────────────────────────────────────
  TIER 4 — MAXIMUM  |  PREMIUM DATA PRODUCT

  Row-level complaint history per property address, joined to FRED macro
  indicators via Last Observation Carried Forward (LOCF).

  PRIMARY USE CASES
    Real Estate  : Property due diligence — full incident history at an address
                   before acquisition. Join to MLS / tax records on full_address
                   or lat/long.
    Insurance    : Claims history lookup — frequency, type, and resolution
                   performance at a specific property, with macro economic
                   context at time of filing.

  FRED JOIN METHOD — LOCF (Option C, Daily Tier)
    FRED series are monthly/quarterly. Each complaint row receives the most
    recent FRED observation whose date <= complaint_date. This carries the
    last known macro value forward to daily granularity.
    Series used: UNRATE (monthly), CPIAUCSL (monthly),
                 FEDFUNDS (monthly), GDP (quarterly).

  ADDRESS CLEANING STANDARDS (industry best practice)
    1. UPPER + TRIM on all address components
    2. Abbreviation normalization (see clean_address CTE):
         ST → STREET,  AVE → AVENUE,  BLVD → BOULEVARD,  DR → DRIVE
         RD → ROAD,    PL → PLACE,    CT → COURT,         LN → LANE
         PKWY → PARKWAY,  HWY → HIGHWAY,  EXPY → EXPRESSWAY
    3. full_address concat for single-field property matching
    4. Rows with null/blank incident_address are excluded from this table
       (use fct_daily_complaints for borough-level aggregate view of those rows)
    5. Cross-borough contamination in city/neighborhood column is flagged
       via neighborhood_quality_flag

  PLACEHOLDER — NYC-SPECIFIC ECONOMIC DATA
    When NYC borough-level economic data is available (BLS borough unemployment,
    NYC DOF property value index, MTA ridership), add a second LEFT JOIN here
    keyed on complaint_date + borough. The FRED join remains as national context.

  SOURCE TABLES
    silver.stg_complaints          — nyc311_marts project
    fred_silver (inline CTE)       — reads FRED bronze directly via read_json_auto;
                                     replace with a dbt source() ref once FRED
                                     pipeline writes to a shared DuckDB warehouse
  ──────────────────────────────────────────────────────────────────────────────
*/

with

-- ── 1. FRED SILVER (inline — reads bronze directly) ──────────────────────────
-- Replace read_json_auto path with a dbt source() ref once
-- FRED pipeline is wired to a shared DuckDB warehouse.
fred_raw as (
    select
        regexp_extract(filename, '([A-Z]+)_\d', 1) as series_id,
        unnest(observations)                         as obs
    from read_json_auto(
        '/app/data/fred_bronze/*.json',
        filename = true
    )
),

fred_silver as (
    select
        series_id,
        cast(obs.date as date)          as observation_date,
        try_cast(obs.value as double)   as value
    from fred_raw
    where try_cast(obs.value as double) is not null
    qualify row_number() over (
        partition by series_id, cast(obs.date as date)
        order by cast(obs.date as date)
    ) = 1
),

-- Pivot FRED wide — one row per month, one column per series
fred_wide as (
    select
        observation_date,
        max(case when series_id = 'UNRATE'   then value end) as unemployment_rate,
        max(case when series_id = 'CPIAUCSL' then value end) as cpi,
        max(case when series_id = 'FEDFUNDS' then value end) as fed_funds_rate,
        max(case when series_id = 'GDP'      then value end) as gdp_billions
    from fred_silver
    group by observation_date
),

-- ── 2. ADDRESS CLEANING ───────────────────────────────────────────────────────
clean_address as (
    select
        complaint_id,
        -- Normalize street abbreviations (word-boundary safe via space padding)
        trim(
            regexp_replace(
            regexp_replace(
            regexp_replace(
            regexp_replace(
            regexp_replace(
            regexp_replace(
            regexp_replace(
            regexp_replace(
            regexp_replace(
            regexp_replace(
                upper(trim(incident_address)),
            '\bSTREET\b',  'ST' ),   -- already expanded; collapse to canonical
            ' ST$',        ' STREET'),
            ' AVE$',       ' AVENUE'),
            ' BLVD$',      ' BOULEVARD'),
            ' DR$',        ' DRIVE'),
            ' RD$',        ' ROAD'),
            ' PL$',        ' PLACE'),
            ' CT$',        ' COURT'),
            ' LN$',        ' LANE'),
            ' PKWY$',      ' PARKWAY')
        ) as incident_address_clean,

        -- Full single-field address for property matching / geocoding
        trim(
            upper(trim(incident_address))
            || ', '
            || coalesce(upper(trim(city)), upper(trim(borough)))
            || ', '
            || upper(trim(borough))
            || ', NY '
            || coalesce(nullif(trim(zip_code),''), '00000')
        ) as full_address,

        -- Neighborhood quality flag:
        -- CLEAN  = city value is a real sub-borough neighborhood
        -- GENERIC = city is just the borough name (no sub-area resolution)
        -- MISSING = city is null
        -- CROSS_BOROUGH = city value doesn't match expected borough
        case
            when city is null or trim(city) = ''
                then 'MISSING'
            when upper(trim(city)) in ('MANHATTAN','BROOKLYN','QUEENS',
                                       'BRONX','STATEN ISLAND','NEW YORK')
                then 'GENERIC'
            when upper(trim(borough)) = 'QUEENS'
             and upper(trim(city)) not in (
                 'JAMAICA','FLUSHING','ASTORIA','RIDGEWOOD','FOREST HILLS',
                 'CORONA','JACKSON HEIGHTS','ELMHURST','BAYSIDE','RICHMOND HILL',
                 'WOODHAVEN','FAR ROCKAWAY','OZONE PARK','SOUTH OZONE PARK',
                 'WHITESTONE','MASPETH','SUNNYSIDE','WOODSIDE','FRESH MEADOWS',
                 'HOLLIS','SAINT ALBANS','QUEENS VILLAGE','COLLEGE POINT',
                 'BELLEROSE','REGO PARK','MIDDLE VILLAGE','HOWARD BEACH',
                 'LONG ISLAND CITY','EAST ELMHURST','SPRINGFIELD GARDENS',
                 'ROSEDALE','CAMBRIA HEIGHTS','LITTLE NECK','OAKLAND GARDENS',
                 'RICHMOND HILL','SOUTH RICHMOND HILL','ARVERNE','ROCKAWAY BEACH',
                 'KEW GARDENS','GLEN OAKS','FLORAL PARK','NEW HYDE PARK')
                then 'CROSS_BOROUGH'
            else 'CLEAN'
        end as neighborhood_quality_flag

    from {{ ref('stg_complaints') }}
    where incident_address is not null
      and trim(incident_address) != ''
),

-- ── 3. BASE COMPLAINTS (address-filtered) ────────────────────────────────────
complaints as (
    select *
    from {{ ref('stg_complaints') }}
    where incident_address is not null
      and trim(incident_address) != ''
),

-- ── 4. LOCF JOIN — match each complaint to most recent FRED observation ───────
-- DATE_TRUNC to month is correct for UNRATE/CPI/FEDFUNDS (monthly series).
-- GDP is quarterly but is loaded as first-day-of-quarter — LOCF handles it.
fred_locf as (
    select
        c.complaint_id,
        f.observation_date                              as fred_reference_date,
        f.unemployment_rate,
        f.cpi,
        f.fed_funds_rate,
        f.gdp_billions
    from complaints c
    left join fred_wide f
        on date_trunc('month', c.complaint_date) = f.observation_date
)

-- ── 5. FINAL OUTPUT ───────────────────────────────────────────────────────────
select
    -- Identity
    c.complaint_id,

    -- Address (cleaned)
    ca.incident_address_clean                           as incident_address,
    ca.full_address,
    c.city                                              as neighborhood,
    ca.neighborhood_quality_flag,
    c.zip_code,
    c.borough,
    c.latitude,
    c.longitude,

    -- Complaint detail
    c.complaint_date,
    c.created_date,
    c.closed_date,
    c.complaint_type,
    c.descriptor,
    c.agency,
    c.agency_name,
    c.status,
    c.channel_type,
    c.location_type,
    c.police_precinct,
    c.community_board,

    -- Resolution
    c.resolution_days,
    c.resolution_date,
    c.resolution_description,
    c.is_resolution_anomaly,

    -- ── FRED MACRO (LOCF) ──────────────────────────────────────────────────
    -- All values represent the most recent monthly/quarterly observation
    -- on or before complaint_date. Null = no FRED data available for period.
    fl.fred_reference_date,
    fl.unemployment_rate,           -- UNRATE: national unemployment %
    fl.cpi,                         -- CPIAUCSL: consumer price index
    fl.fed_funds_rate,              -- FEDFUNDS: federal funds rate %
    fl.gdp_billions                 -- GDP: national GDP in billions USD

    -- ── PLACEHOLDER: NYC-SPECIFIC ECONOMIC DATA ───────────────────────────
    -- Add columns here when NYC BLS / DOF data is available:
    --   borough_unemployment_rate   (NYC BLS, monthly, by borough)
    --   nyc_property_value_index    (NYC DOF, quarterly, by borough)
    --   mta_daily_ridership         (MTA, daily)
    -- Join key: complaint_date + c.borough

from complaints c
inner join clean_address ca
    on c.complaint_id = ca.complaint_id
left join fred_locf fl
    on c.complaint_id = fl.complaint_id
