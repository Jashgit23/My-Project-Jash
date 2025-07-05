{{ config(materialized='table') }}

WITH orders2023 AS (
  SELECT
    orders_id,
    customers_id,
    date_date
  FROM {{ ref('dlk_orders') }}
  WHERE EXTRACT(YEAR FROM date_date) = 2023
),

customer_order_history_12m AS (
  SELECT
    o2023.orders_id AS current_order_id,
    o2023.customers_id,
    o2023.date_date AS current_order_date,
    past.orders_id AS prior_order_id,
    past.date_date AS prior_order_date
  FROM orders2023 AS o2023
  LEFT JOIN {{ ref('dlk_orders') }} AS past
    ON o2023.customers_id = past.customers_id
    AND past.date_date < o2023.date_date
    AND past.date_date >= DATE_SUB(o2023.date_date, INTERVAL 12 MONTH)
)

SELECT * FROM customer_order_history_12m
ORDER BY customers_id, current_order_date, prior_order_date
