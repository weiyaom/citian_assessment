{{ config(materialized="table") }}

with all_times as (
    select pickup_time as t from {{ ref('stg_trips') }}
    union
    select dropoff_time from {{ ref('stg_trips') }}
),

expanded as (
    select
        t as time_id,
        date(t) as date,
        extract(year from t) as year,
        extract(month from t) as month,
        extract(day from t) as day,
        extract(hour from t) as hour,
        extract(dow from t) as weekday
    from all_times
)

select * from expanded;
