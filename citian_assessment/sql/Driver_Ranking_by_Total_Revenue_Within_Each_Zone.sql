SELECT
    l.zone,
    f.driver_id,
    SUM(f.revenue) AS total_revenue,
    RANK() OVER (
        PARTITION BY l.zone
        ORDER BY SUM(f.revenue) DESC
    ) AS revenue_rank
FROM fact_trips f
JOIN dim_locations l
    ON f.pickup_location_id = l.location_id
GROUP BY l.zone, f.driver_id
ORDER BY l.zone, revenue_rank;
