{{ config(materialized='table') }}
SELECT
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "payment_type",
    date_trunc('day', "tpep_pickup_datetime") AS trip_date
FROM {{ source('raw', 'yellow_trips_clean') }}
