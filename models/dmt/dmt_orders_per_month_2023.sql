{{ config(materialized='table') }}

SELECT
  EXTRACT(MONTH FROM date_date) AS month,
  COUNT(*) AS orders_count
FROM {{ ref('dlk_orders') }}
WHERE EXTRACT(YEAR FROM date_date) = 2023
GROUP BY month
ORDER BY month
