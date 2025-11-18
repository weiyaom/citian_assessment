{{ config(materialized='table') }}
WITH locations AS (
    SELECT PULocationID as location_id
    FROM {{ source('raw', 'yellow_trip_data_2022') }}
    UNION DISTINCT
    SELECT DOLocationID as location_id
    FROM {{ source('raw', 'yellow_trip_data_2022') }}
)
SELECT distinct location_id
FROM locations
ORDER BY location_id
