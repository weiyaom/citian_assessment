{{ config(
    materialized='incremental',
    unique_key='trip_id'
) }}

with base as (
    select
        row_number() over (order by pickup_time) as trip_id,
        pickup_loc_id,
        dropoff_loc_id,
        pickup_time,
        dropoff_time,
        passenger_count,
        trip_distance,
        fare_amount,
        total_amount
    from {{ ref('stg_trips') }}
),

final as (
    select
        b.trip_id,
        b.pickup_loc_id,
        b.dropoff_loc_id,
        t1.time_id as pickup_time_id,
        t2.time_id as dropoff_time_id,
        b.passenger_count,
        b.trip_distance,
        b.fare_amount,
        b.total_amount
    from base b
    left join {{ ref('dim_time') }} t1
        on b.pickup_time = t1.time_id
    left join {{ ref('dim_time') }} t2
        on b.dropoff_time = t2.time_id
)

select * from final

{% if is_incremental() %}
where trip_id > (select max(trip_id) from {{ this }})
{% endif %}
;
