{{ config(materialized='view') }}

select
    -- keys for dimension tables
    "PULocationID" as pickup_loc_id,
    "DOLocationID" as dropoff_loc_id,

    -- datetime
    cast("tpep_pickup_datetime" as timestamp) as pickup_time,
    cast("tpep_dropoff_datetime" as timestamp) as dropoff_time,

    -- measures
    cast("passenger_count" as int) as passenger_count,
    cast("trip_distance" as float) as trip_distance,
    cast("fare_amount" as float) as fare_amount,
    cast("total_amount" as float) as total_amount

from {{ source('raw', 'yellow_tripdata_raw') }}
where
    trip_distance > 0
    and passenger_count >= 1
    and fare_amount >= 0
    and total_amount >= 0
    and tpep_pickup_datetime is not null
    and tpep_dropoff_datetime is not null
    and tpep_dropoff_datetime > tpep_pickup_datetime
;
