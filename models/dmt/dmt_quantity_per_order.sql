{{ config(materialized='table') }}

SELECT
  order_id,
  customer_id,
  date_date,
  SUM(qty) AS qty_product
FROM {{ ref('dlk_sales') }}
WHERE EXTRACT(YEAR FROM date_date) IN (2022, 2023)
GROUP BY order_id, customer_id, date_date
ORDER BY date_date
