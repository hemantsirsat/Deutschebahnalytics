{{ config(
    materialized='table'
) }}

-- stg_timetables: compute delay and copy all raw columns
select
    *,
    (actual_time - scheduled_time) as delay
from {{ source('raw', 'raw_timetables') }} WHERE (actual_time IS NOT NULL AND scheduled_time IS NOT NULL)
