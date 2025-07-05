{{ config(
    materialized='table',
) }}

SELECT
    COUNT(*) AS total_orders_2023
FROM
    {{ ref('dlk_orders') }}
WHERE
    EXTRACT(YEAR FROM date_date) = 2023
