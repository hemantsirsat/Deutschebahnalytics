{{ config(
    materialized='incremental',
    unique_key=['date', 'hour', 'station_name']
) }}

with base as (
    select
        station_name,
        coalesce(planned_arrival_time, planned_departure_time) as reference_time,
        arrival_delay,
        departure_delay
    from {{ ref('int_station_delay') }}
    {% if is_incremental() %}
        where coalesce(planned_arrival_time, planned_departure_time) > 
              (select max(reference_time) from {{ this }})
    {% endif %}
),

transformed as (
    select
        station_name,
        cast(date_trunc('day', reference_time) as date) as date,
        extract(hour from reference_time) as hour,
        extract(epoch from arrival_delay::interval)/60.0 as arrival_delay_min,
        extract(epoch from departure_delay::interval)/60.0 as departure_delay_min,
        reference_time
    from base
),

aggregated as (
    select
        date,
        hour,
        station_name,
        avg(arrival_delay_min) as avg_arrival_delay_min,
        avg(departure_delay_min) as avg_departure_delay_min,
        max(reference_time) as reference_time
    from transformed
    group by 1,2,3
)

select
    date,
    hour,
    station_name,
    avg_arrival_delay_min,
    avg_departure_delay_min,
    reference_time
from aggregated
order by date, hour, station_name
