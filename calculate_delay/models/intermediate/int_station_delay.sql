{{ config(materialized='table') }}

SELECT
    s.eva_number,
    si.name AS station_name,
    s.planned_arrival_time,
    s.planned_departure_time,
    s.arrival_delay,
    s.departure_delay
FROM {{ ref("stg_timetables") }} s
LEFT JOIN {{ source("raw", "raw_stations") }} si
    ON CAST(s.eva_number AS BIGINT) = si.eva_number
