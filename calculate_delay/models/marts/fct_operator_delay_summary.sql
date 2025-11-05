{{ config(
    materialized='table',
    tags=['mart']
) }}

SELECT
    train_operator,
    avg_arrival_delay_min,
    avg_departure_delay_min,
    total_delays
FROM {{ ref('int_operator_delay') }}
GROUP BY train_operator
ORDER BY total_delays DESC
