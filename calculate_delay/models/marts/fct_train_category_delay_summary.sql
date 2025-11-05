{{ config(
    materialized='table',
    tags=['mart']
) }}

SELECT
    train_category,
    avg_arrival_delay_min,
    avg_departure_delay_min,
    total_delays
FROM {{ ref('int_train_category_delay') }}
ORDER BY total_delays DESC
