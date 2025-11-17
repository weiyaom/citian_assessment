WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY date_trunc('day', tpep_pickup_datetime)
            ORDER BY tip_amount DESC
        ) AS rn
    FROM yellow_trips_clean
)
SELECT *
FROM ranked
WHERE rn = 1;


SELECT
    payment_type,
    COUNT(*) AS total_trips
FROM yellow_trips_clean
GROUP BY 1
ORDER BY 2 DESC;


SELECT
    date_trunc('hour', tpep_pickup_datetime) AS hour,
    AVG(trip_distance) AS avg_dist
FROM yellow_trips_clean
GROUP BY 1
ORDER BY 1;
