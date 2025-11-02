{{ config(
    materialized='table'
) }}

WITH stg AS (
    SELECT
        eva_number,
        arrival_delay,
        departure_delay
    FROM {{ ref("stg_timetables") }}
)

SELECT
    eva_number,
    ROUND(AVG(EXTRACT(EPOCH FROM arrival_delay) / 60.0), 2) AS avg_arrival_delay_min,
    ROUND(AVG(EXTRACT(EPOCH FROM departure_delay) / 60.0), 2) AS avg_departure_delay_min,
    COUNT(*) AS total_delays
FROM stg
WHERE arrival_delay IS NOT NULL OR departure_delay IS NOT NULL
GROUP BY eva_number
ORDER BY eva_number