{{ config(materialized='table') }}

SELECT
  current_order_id,
  customers_id,
  current_order_date,
  COUNT(prior_order_id) AS prior_12m_orders,
  CASE
    WHEN COUNT(prior_order_id) = 0 THEN 'New'
    WHEN COUNT(prior_order_id) BETWEEN 1 AND 3 THEN 'Returning'
    ELSE 'VIP'
  END AS order_segmentation
FROM {{ ref('customer_order2023_history_12m') }}
GROUP BY current_order_id, customers_id, current_order_date
ORDER BY customers_id, current_order_date
