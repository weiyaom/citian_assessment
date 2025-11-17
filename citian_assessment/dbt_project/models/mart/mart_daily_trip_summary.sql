{{ config(materialized='table') }}

-- ===============================================================
-- MART MODEL: Daily Trip Summary
-- Aggregated metrics for Yellow Taxi trips
-- Includes common SQL functions likely to appear in exams:
--   * Aggregates: COUNT, SUM, AVG, MAX, MIN
--   * Date/time: date_trunc, extract
--   * Window functions: ROW_NUMBER, DENSE_RANK, LAG
--   * Conditional logic: CASE WHEN
--   * Casting: CAST
--   * NULL handling: COALESCE
-- ===============================================================

WITH base AS (

    SELECT
        -- ======================
        -- Core fields
        -- ======================
        "VendorID" AS vendor_id,
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "payment_type",

        -- ======================
        -- Derived fields
        -- ======================
        date_trunc('day', "tpep_pickup_datetime") AS trip_date,           -- daily aggregation
        extract(hour from "tpep_pickup_datetime") AS pickup_hour,         -- hour extraction
        CASE WHEN "trip_distance" > 0 THEN "fare_amount" / "trip_distance" END AS fare_per_mile,
        COALESCE("passenger_count", 0) AS passenger_count_clean,          -- null handling

        -- ======================
        -- Window function examples
        -- ======================
        ROW_NUMBER() OVER (PARTITION BY "VendorID" ORDER BY "tpep_pickup_datetime") AS rn,
        DENSE_RANK() OVER (PARTITION BY date_trunc('day', "tpep_pickup_datetime") ORDER BY "fare_amount" DESC) AS fare_rank,
        LAG("fare_amount") OVER (PARTITION BY "VendorID" ORDER BY "tpep_pickup_datetime") AS prev_fare_amount

    FROM {{ source('raw', 'yellow_trips_clean') }}

    -- ======================
    -- Optional filters (data cleaning)
    -- ======================
    WHERE "trip_distance" > 0
      AND "fare_amount" >= 0
      AND "passenger_count" >= 0

)

-- ===============================================================
-- Daily aggregation with examples of COUNT, AVG, SUM
-- ===============================================================
SELECT
    trip_date,
    COUNT(*) AS total_trips,
    SUM("trip_distance") AS total_distance,
    AVG("trip_distance") AS avg_trip_distance,
    AVG("fare_amount") AS avg_fare_amount,
    SUM("fare_amount") AS total_fare,
    AVG("tip_amount") AS avg_tip,
    MAX("fare_amount") AS max_fare,
    MIN("fare_amount") AS min_fare,
    COUNT(DISTINCT vendor_id) AS unique_vendors,
    MAX(prev_fare_amount) AS last_fare_amount

FROM base

GROUP BY 1
ORDER BY 1
