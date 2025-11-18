{{ config(materialized='view') }}
with hourly_counts AS (
    SELECT PULocationID as zone_id,
    EXTRACT (hour from tpep_pickup_datetime ) AS hour_of_day,
    count(*) as trip_count
    FROM  yellow_trip_data_2022
    GROUP BY PULocationID, hour_of_day
),
ranked as (
    SELECT zone_id, hour_of_day, trip_count,
    RANK() OVER (
    PARTITION BY zone_id
    ORDER BY trip_count DESC
    ) as hour_rank
    FROM hourly_counts
)

SELECT zone_id, hour_of_day, trip_count
FROM ranked
WHERE rank_hour = 1
ORDER BY zone_id
