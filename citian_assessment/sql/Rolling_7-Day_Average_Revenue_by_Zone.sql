WITH daily_revenue AS (
    SELECT
        t.date,
        l.zone,
        SUM(f.revenue) AS total_revenue
    FROM fact_trips f
    JOIN dim_time t
        ON f.time_id = t.time_id
    JOIN dim_locations l
        ON f.pickup_location_id = l.location_id
    GROUP BY t.date, l.zone
)

SELECT
    date,
    zone,
    total_revenue,
    AVG(total_revenue) OVER (
        PARTITION BY zone
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_avg_revenue
FROM daily_revenue
ORDER BY zone, date;
