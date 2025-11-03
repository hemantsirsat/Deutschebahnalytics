{{ config(
    materialized='table'
) }}

with base as (
    select
        extract(hour from coalesce(actual_arrival_time, actual_departure_time))::int as hour_of_day,
        arrival_delay,
        departure_delay
    from {{ ref('stg_timetables') }}
    where arrival_delay is not null or departure_delay is not null
)

select
    hour_of_day,
    count(case when arrival_delay is not null then 1 end) as arrival_delay_count,
    count(case when departure_delay is not null then 1 end) as departure_delay_count,
    round(avg(extract(epoch from arrival_delay) / 60.0), 2) as avg_arrival_delay_min,
    round(avg(extract(epoch from departure_delay) / 60.0), 2) as avg_departure_delay_min
from base
group by hour_of_day
order by hour_of_day
