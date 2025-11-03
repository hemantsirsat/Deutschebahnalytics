{{ config(
    materialized='table'
) }}

WITH stg AS (
    SELECT
        eva_number,
        arrival_delay,
        departure_delay
    FROM {{ ref("stg_timetables") }}
),

station_info AS (
    SELECT
        eva_number,
        name AS station_name
    FROM {{ source("raw", "raw_stations") }}
)

SELECT
    s.eva_number,
    si.station_name,
    ROUND(AVG(EXTRACT(EPOCH FROM s.arrival_delay) / 60.0), 2) AS avg_arrival_delay_min,
    ROUND(AVG(EXTRACT(EPOCH FROM s.departure_delay) / 60.0), 2) AS avg_departure_delay_min,
    COUNT(*) AS total_delays
FROM stg s
LEFT JOIN station_info si
    ON CAST(s.eva_number AS BIGINT) = si.eva_number
WHERE s.arrival_delay IS NOT NULL OR s.departure_delay IS NOT NULL
GROUP BY s.eva_number, si.station_name
ORDER BY s.eva_number
