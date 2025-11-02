{{ config(
    materialized='table'
) }}

-- stg_timetables: compute delay and copy all raw columns
select
    *,
    case 
        when actual_arrival_time is not null and planned_arrival_time is not null 
        then actual_arrival_time - planned_arrival_time
        else null
    end as arrival_delay,
    case 
        when actual_departure_time is not null and planned_departure_time is not null
        then actual_departure_time - planned_departure_time
        else null
    end as departure_delay
from {{ source('raw', 'raw_timetable') }}
where (actual_arrival_time is not null and planned_arrival_time is not null)
   or (actual_departure_time is not null and planned_departure_time is not null)