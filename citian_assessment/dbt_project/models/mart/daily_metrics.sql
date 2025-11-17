SELECT
    trip_date,
    COUNT(*) AS total_trips,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(fare_amount) AS avg_fare_amount
FROM {{ ref('stg_yellow_trips') }}
GROUP BY 1
ORDER BY 1
