{{ config(
    materialized='table'
) }}

select
    hour_of_day,
    arrival_delay_count,
    departure_delay_count,
    avg_arrival_delay_min,
    avg_departure_delay_min,
    (arrival_delay_count + departure_delay_count) as total_delays
from {{ ref('int_hourly_delay_count') }}
order by hour_of_day
