{{ config(
    materialized='table',
    tags=['mart']
) }}

SELECT
    station_name,
    ROUND(AVG(EXTRACT(EPOCH FROM arrival_delay) / 60.0), 2) AS avg_arrival_delay_min,
    ROUND(AVG(EXTRACT(EPOCH FROM departure_delay) / 60.0), 2) AS avg_departure_delay_min,
    COUNT(*) AS total_delays
FROM {{ ref('int_station_delay') }}
GROUP BY station_name
ORDER BY station_name
