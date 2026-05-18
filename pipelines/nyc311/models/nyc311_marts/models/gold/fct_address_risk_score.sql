{{ config(
    materialized='table',
    schema='gold'
) }}

/*
  fct_address_risk_score
  ──────────────────────────────────────────────────────────────────────────────
  TIER 4 — MAXIMUM  |  PREMIUM DATA PRODUCT

  Aggregated complaint intelligence per property address.
  One row per unique cleaned address + ZIP combination.

  PRIMARY USE CASES
    Real Estate  : Acquisition screening — risk-score a target property or
                   portfolio against neighborhood complaint history before bid.
    Insurance    : Underwriting input — property risk score as a feature in
                   premium pricing models. Flag high-frequency anomaly addresses.

  RISK SCORE (v1 — calibrate with actuary/underwriter before production use)
  ──────────────────────────────────────────────────────────────────────────────
  Formula:
    base_score = (total_complaints × 2)
               + (open_complaints  × 5)       ← unresolved = higher weight
               + (avg_resolution_days × 0.5)  ← slow resolution = risk signal
               + (anomaly_count × 10)         ← data anomalies = highest weight
    property_risk_score = LEAST(100, base_score)

  Risk Tiers:
    HIGH    ≥ 70  — Material risk. Recommend site inspection + claims review.
    MEDIUM  40–69 — Moderate risk. Include in underwriting notes.
    LOW     < 40  — Standard risk. No elevated flag.

  CALIBRATION NOTE
    Weights above are v1 illustrative values. To calibrate:
      1. Join to actual insurance loss data by address
      2. Run logistic regression or gradient boosting on base features
      3. Replace hardcoded weights with model-derived coefficients
      4. Validate lift curve against holdout set

  COMPLAINT TYPE RISK CLASSIFICATION
    High-risk types (structural/habitability signals):
      HEAT/HOT WATER, PLUMBING, PAINT/PLASTER, DOOR/WINDOW,
      WATER LEAK, ELEVATOR, MOLD, RODENTS, UNSANITARY CONDITION
    Medium-risk types (operational/nuisance signals):
      NOISE, ILLEGAL PARKING, BLOCKED DRIVEWAY, SANITATION
    Low-risk types (environmental/transient signals):
      DAMAGED TREE, STANDING WATER, STREET LIGHT

  SOURCE TABLES
    gold.fct_property_incidents    — uses cleaned address + all complaint fields
    (or silver.stg_complaints directly for lighter dependency)
  ──────────────────────────────────────────────────────────────────────────────
*/

with

-- ── 1. ADDRESS CLEANING (same logic as fct_property_incidents) ────────────────
clean_base as (
    select
        complaint_id,
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
            '\bSTREET\b',  'ST'),
            ' ST$',        ' STREET'),
            ' AVE$',       ' AVENUE'),
            ' BLVD$',      ' BOULEVARD'),
            ' DR$',        ' DRIVE'),
            ' RD$',        ' ROAD'),
            ' PL$',        ' PLACE'),
            ' CT$',        ' COURT'),
            ' LN$',        ' LANE'),
            ' PKWY$',      ' PARKWAY')
        )                                               as incident_address_clean,

        trim(
            upper(trim(incident_address))
            || ', '
            || coalesce(upper(trim(city)), upper(trim(borough)))
            || ', '
            || upper(trim(borough))
            || ', NY '
            || coalesce(nullif(trim(zip_code),''), '00000')
        )                                               as full_address,

        upper(trim(city))                               as neighborhood,
        upper(trim(borough))                            as borough,
        nullif(trim(zip_code), '')                      as zip_code,
        try_cast(latitude  as double)                   as latitude,
        try_cast(longitude as double)                   as longitude,
        complaint_type,
        status,
        complaint_date,
        resolution_days,
        is_resolution_anomaly,
        channel_type,
        police_precinct,
        community_board,

        -- Complaint type risk classification
        case
            when complaint_type in (
                'HEAT/HOT WATER','PLUMBING','PAINT/PLASTER','DOOR/WINDOW',
                'WATER LEAK','ELEVATOR','MOLD','RODENTS',
                'UNSANITARY CONDITION','PEST CONTROL','LEAD','ASBESTOS',
                'ELECTRIC','GAS','FIRE SAFETY','STRUCTURAL',
                'CEILING','FLOORING','GENERAL CONSTRUCTION'
            ) then 'HIGH'
            when complaint_type in (
                'NOISE - RESIDENTIAL','NOISE - COMMERCIAL','NOISE - STREET/SIDEWALK',
                'NOISE','ILLEGAL PARKING','BLOCKED DRIVEWAY',
                'SANITATION CONDITION','ILLEGAL DUMPING',
                'GRAFFITI','DERELICT VEHICLE'
            ) then 'MEDIUM'
            else 'LOW'
        end                                             as complaint_risk_class

    from {{ ref('stg_complaints') }}
    where incident_address is not null
      and trim(incident_address) != ''
),

-- ── 2. TOP COMPLAINT TYPE per address ────────────────────────────────────────
top_complaint as (
    select
        incident_address_clean,
        zip_code,
        complaint_type                                  as top_complaint_type,
        count(*)                                        as type_count
    from clean_base
    group by incident_address_clean, zip_code, complaint_type
    qualify row_number() over (
        partition by incident_address_clean, zip_code
        order by count(*) desc
    ) = 1
),

-- ── 3. HIGHEST RISK CLASS per address ────────────────────────────────────────
-- If an address has ever had a HIGH-risk complaint type, flag it
highest_risk_class as (
    select
        incident_address_clean,
        zip_code,
        case
            when max(case when complaint_risk_class = 'HIGH'   then 3 else 0 end) = 3 then 'HIGH'
            when max(case when complaint_risk_class = 'MEDIUM' then 2 else 0 end) = 2 then 'MEDIUM'
            else 'LOW'
        end                                             as highest_complaint_risk_class
    from clean_base
    group by incident_address_clean, zip_code
),

-- ── 4. AGGREGATE METRICS per address ─────────────────────────────────────────
agg as (
    select
        incident_address_clean,
        full_address,
        neighborhood,
        borough,
        zip_code,
        police_precinct,
        community_board,

        -- Geo (average where multiple observations — should be stable per address)
        round(avg(latitude),  6)                        as latitude,
        round(avg(longitude), 6)                        as longitude,

        -- Volume
        count(*)                                        as total_complaints,
        count(distinct complaint_date)                  as complaint_days,
        count(distinct complaint_type)                  as distinct_complaint_types,

        -- Status
        sum(case when status != 'CLOSED' then 1 else 0 end)
                                                        as open_complaints,
        round(
            sum(case when status != 'CLOSED' then 1.0 else 0 end)
            / count(*) * 100, 1)                        as open_rate_pct,

        -- Resolution
        round(avg(
            case when resolution_days >= 0 then resolution_days end
        ), 1)                                           as avg_resolution_days,
        max(case when resolution_days >= 0 then resolution_days end)
                                                        as max_resolution_days,

        -- Anomalies
        sum(case when is_resolution_anomaly then 1 else 0 end)
                                                        as anomaly_count,
        round(
            sum(case when is_resolution_anomaly then 1.0 else 0 end)
            / count(*) * 100, 2)                        as anomaly_rate_pct,

        -- Risk class breakdown
        sum(case when complaint_risk_class = 'HIGH'   then 1 else 0 end)
                                                        as high_risk_complaints,
        sum(case when complaint_risk_class = 'MEDIUM' then 1 else 0 end)
                                                        as medium_risk_complaints,
        sum(case when complaint_risk_class = 'LOW'    then 1 else 0 end)
                                                        as low_risk_complaints,

        -- Channel breakdown
        sum(case when channel_type = 'PHONE'  then 1 else 0 end)
                                                        as phone_complaints,
        sum(case when channel_type = 'ONLINE' then 1 else 0 end)
                                                        as online_complaints,
        sum(case when channel_type = 'MOBILE' then 1 else 0 end)
                                                        as mobile_complaints,

        -- Time range
        min(complaint_date)                             as first_complaint_date,
        max(complaint_date)                             as last_complaint_date,
        datediff('day', min(complaint_date), max(complaint_date))
                                                        as complaint_history_days

    from clean_base
    group by
        incident_address_clean, full_address, neighborhood,
        borough, zip_code, police_precinct, community_board
),

-- ── 5. RISK SCORE CALCULATION ─────────────────────────────────────────────────
scored as (
    select
        a.*,
        tc.top_complaint_type,
        hrc.highest_complaint_risk_class,

        -- v1 Risk Score (see calibration note in header)
        round(least(100,
            (a.total_complaints   * 2.0)
          + (a.open_complaints    * 5.0)
          + (coalesce(a.avg_resolution_days, 0) * 0.5)
          + (a.anomaly_count      * 10.0)
          + (a.high_risk_complaints * 3.0)    -- additional weight for structural types
        ), 1)                                               as property_risk_score

    from agg a
    left join top_complaint tc
        on a.incident_address_clean = tc.incident_address_clean
       and a.zip_code               = tc.zip_code
    left join highest_risk_class hrc
        on a.incident_address_clean = hrc.incident_address_clean
       and a.zip_code               = hrc.zip_code
)

-- ── 6. FINAL OUTPUT ───────────────────────────────────────────────────────────
select
    -- Identity / Geography
    incident_address_clean                              as incident_address,
    full_address,
    neighborhood,
    borough,
    zip_code,
    police_precinct,
    community_board,
    latitude,
    longitude,

    -- Risk Score + Tier
    property_risk_score,
    case
        when property_risk_score >= 70 then 'HIGH'
        when property_risk_score >= 40 then 'MEDIUM'
        else                                'LOW'
    end                                                 as risk_tier,
    highest_complaint_risk_class,

    -- Volume
    total_complaints,
    complaint_days,
    distinct_complaint_types,
    top_complaint_type,

    -- Status
    open_complaints,
    open_rate_pct,

    -- Resolution
    avg_resolution_days,
    max_resolution_days,

    -- Anomalies
    anomaly_count,
    anomaly_rate_pct,

    -- Risk class breakdown
    high_risk_complaints,
    medium_risk_complaints,
    low_risk_complaints,

    -- Channel breakdown
    phone_complaints,
    online_complaints,
    mobile_complaints,

    -- Time range
    first_complaint_date,
    last_complaint_date,
    complaint_history_days,

    -- Metadata
    current_timestamp                                   as scored_at

from scored
order by property_risk_score desc, total_complaints desc
