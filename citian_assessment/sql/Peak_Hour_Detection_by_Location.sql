WITH hourly_counts AS (
    SELECT
        l.zone,
        t.hour,
        COUNT(*) AS trip_count,
        ROW_NUMBER() OVER (
            PARTITION BY l.zone
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM fact_trips f
    JOIN dim_time t
        ON f.time_id = t.time_id
    JOIN dim_locations l
        ON f.pickup_location_id = l.location_id
    GROUP BY l.zone, t.hour
)

SELECT zone, hour AS peak_hour, trip_count
FROM hourly_counts
WHERE rn = 1
ORDER BY zone;
