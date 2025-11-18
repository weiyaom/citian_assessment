{{ config(materialized='table') }}
SELECT
    DATE_TRUNC('day', tpep_pickup_datetime) as pickup_day,
    DATE_TRUNC('day', tpep_dropoff_datetime) as dropoff_day
FROM {{ source('raw', 'yellow_trip_data_2022') }}

--TODO final all time related data and put into this time_dim table
