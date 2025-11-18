{{ config(materialized="table") }}

with unique_locs as (
    select pickup_loc_id as location_id from {{ ref('stg_trips') }}
    union
    select dropoff_loc_id from {{ ref('stg_trips') }}
),

enriched as (
    select
        u.location_id,
        z.borough,
        z.zone,
        z.service_zone
    from unique_locs u
    left join {{ source('raw', 'taxi_zone_lookup') }} z
        on u.location_id = z.location_id
)

select * from enriched;
