{{ config(materialized='table') }}

SELECT
  EXTRACT(MONTH FROM date_date) AS month,
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(qty) AS total_quantity,
  ROUND(SUM(qty) / COUNT(DISTINCT order_id), 2) AS avg_products_per_order
FROM {{ ref('dlk_sales') }}
WHERE EXTRACT(YEAR FROM date_date) = 2023
GROUP BY month
ORDER BY month
