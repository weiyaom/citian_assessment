{{ config(materialized='table') }}
SELECT
    {{ dbt_utils.generate_surrogate_key ([
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    ]) --make this a unique key
    }} as trip_unique_key,
    "tip_amount "
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "payment_type"
FROM {{ source('raw', 'yellow_trip_data_2022') }}
