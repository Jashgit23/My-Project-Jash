Take-Home challenge
DOCUMENTATION
BIGQUERY SETUP
Created a sandbox account in Bigquery 
Created a dataset under my project
Injected both the CSV files for assignment as SALES and ORDERS table.
Under API& Services created a credentials  to connect bigquery and DBT through service account and downloaded the JSON file with all security details.

DBT SETUP
Create a free account in DBT
Integrate the Github through integration services
Connected Bigquery as project and uploaded the JSON file to establish connection

DBT WORKFLOW
Create the dataset dbt_jashgit23 where all the transformed data will be loaded
The model is split into 3 DLK, DWH, DMT
DLK is for import of the entire raw data
DWH is for all the cleaning transformation referencing the DWH
DMT is the final cleaned data with all required columns for reporting
All code changes are pushed to github and merged on review

GITHUB
Set up Github account -  https://github.com/Jashgit23/My-Project-Jash
The readme has all the SQL and LookML code
Drafted a new release t know the current version 2023.2


LookML
Trained LookML coding in Qwiklabs
Structured the semantic model in two steps model and views
Model file is the entire collection of tables where the relation between tables is defined
Views is the definition of each table where things like datatype, custom calculated fields is defined

LookerStudio Dashboard -
https://lookerstudio.google.com/reporting/bfe4c9bc-6447-4c37-bfee-caadb6b1ee69




# SQL and LOOKML

--Exercice 1:
--What is the number of orders in the year 2023?
SELECT COUNT(*) AS total_orders_2023
FROM `Assignment.Orders`
WHERE EXTRACT(YEAR FROM date_date) = 2023




-- Exercice 2:
-- What is the number of orders per month in the year 2023?
SELECT
  EXTRACT(MONTH FROM date_date) AS month,
  COUNT(*) AS orders_count
FROM `Assignment.Orders`
WHERE EXTRACT(YEAR FROM date_date) = 2023
GROUP BY month
ORDER BY month


-- Exercice 3:
-- What is the average number of products per order for each month of the year 2023?
SELECT
  EXTRACT(MONTH FROM date_date) AS month,
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(qty) AS total_quantity,
  ROUND(SUM(qty) / COUNT(DISTINCT order_id), 2) AS avg_products_per_order
FROM `Assignment.Sales`
WHERE EXTRACT(YEAR FROM date_date) = 2023
GROUP BY month
ORDER BY month;




-- Exercice 4:
-- Create a table (1 line per order) for all orders in the year 2022 and 2023; this table
SELECT
  order_id,
  customer_id,
  date_date,
  SUM(qty) AS qty_product
FROM `Assignment.Sales`
WHERE EXTRACT(YEAR FROM date_date) IN (2022, 2023)
GROUP BY order_id, customer_id, date_date
ORDER BY date_date
-- Exercice 5:
WITH orders2023 AS (
  SELECT
    orders_id,
    customers_id,
    date_date
  FROM `Assignment.Orders`
  WHERE EXTRACT(YEAR FROM date_date) = 2023
),


activity_orders AS (
  SELECT
    o2023.orders_id AS orders_id,
    o2023.customers_id AS customers_id,
    o2023.date_date AS order_date,
    COUNT(past.orders_id) AS prior_12m_orders
  FROM orders2023 AS o2023
  LEFT JOIN `Assignment.Orders` AS past
    ON o2023.customers_id = past.customers_id
    AND past.date_date < o2023.date_date
    AND past.date_date >= DATE_SUB(o2023.date_date, INTERVAL 12 MONTH)
  GROUP BY o2023.orders_id, o2023.customers_id, o2023.date_date
)


SELECT
  orders_id,
  customers_id,
  order_date,
  prior_12m_orders,
  CASE
    WHEN prior_12m_orders = 0 THEN 'New'
    WHEN prior_12m_orders BETWEEN 1 AND 3 THEN 'Returning'
    ELSE 'VIP'
  END AS order_segmentation
FROM activity_orders
ORDER BY customers_id, order_date;









LOOKML SEMANTIC MODEL -

connection: "bigquery_public_data_looker"  # Connect to your BigQuery project

include: "/views/*.view"                    # Include all view files
include: "/z_tests/*.lkml"                  # Include test files if any
include: "/**/*.dashboard"                   # Include all dashboards

datagroup: sales_default_datagroup {        # Define cache invalidation rules
  max_cache_age: "1 hour"                    # Cache refresh every hour
}

persist_with: sales_default_datagroup        

label: "Customer Order Segmentation"         # Model label in Looker UI

explore: dlk_orders {                         # Base explore on dlk_orders table
  label: "dlk_orders"                         # Label for the explore

  join: dlk_sales {                           # Join sales data to orders
    type: left_outer
    sql_on: ${dlk_orders.order_id} = ${dlk_sales.order_id} ;;
    relationship: one_to_one
  }

  join: order_segmentation {                  # Join order segmentation data
    type: left_outer
    sql_on: ${dlk_orders.order_id} = ${order_segmentation.current_order_id} ;;
    relationship: one_to_one
  }
}




LOOKER VIEWS
DLK_ORDERS 

view: dlk_orders {
  sql_table_name: `hybrid-dominion-464912-s5.dbt_jashgit23.dlk_orders` ;;  
 
  dimension: orders_id {
    primary_key: yes                    # Unique key for this table
    type: string                       # Assuming order_id is a string, change if needed
    sql: ${TABLE}.orders_id ;;
  }

  dimension: customer_id {
    type: string                       # Assuming customer_id is string, change if needed
    sql: ${TABLE}.customer_id ;;
  }

  dimension: date_date {
    type: date                        # Date field for order date
    sql: ${TABLE}.date_date ;;
  }

  dimension: net_sales {
    type: number                      # Numeric field for sales value
    sql: ${TABLE}.net_sales ;;
    value_format_name: "usd"          # Format as currency (USD)
  }

  # Measures (aggregations)
  measure: total_orders {
    type: count                      # Count of orders
    drill_fields: [orders_id, customer_id, date_date]   # Fields to see when drilling down
  }

  measure: total_sales {
    type: sum                       # Sum of net sales
    sql: ${net_sales} ;;
    value_format_name: "usd"
  }
}

DLK_SALES


view: dlk_sales {
  sql_table_name: `hybrid-dominion-464912-s5.dbt_jashgit23.dlk_sales` ;;  # Your BigQuery table

  dimension: order_id {
    type: number
    sql: ${TABLE}.order_id ;;
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
  }

  dimension: date_date {
    type: date
    sql: ${TABLE}.date_date ;;
  }

  dimension: products_id {
    type: number
    sql: ${TABLE}.products_id ;;
  }

  dimension: qty {
    type: number
    sql: ${TABLE}.qty ;;
  }

  dimension: net_sales {
    type: number
    sql: ${TABLE}.net_sales ;;
    value_format_name: "usd"  # Format as currency
  }

  measure: total_quantity {
    type: sum
    sql: ${qty} ;;
  }

  measure: total_net_sales {
    type: sum
    sql: ${net_sales} ;;
    value_format_name: "usd"
  }

  measure: order_count {
    type: count_distinct
    sql: ${order_id} ;;
  }
}

order_segmentation
view: order_segmentation {
  sql_table_name: `dbt_jashgit23.order_segmentation` ;;

  dimension: current_order_id {
    type: number
    sql: ${TABLE}.current_order_id ;;
  }

  dimension: customers_id {
    type: number
    sql: ${TABLE}.customers_id ;;
  }

  dimension: current_order_date {
    type: date
    sql: ${TABLE}.current_order_date ;;
  }

  dimension: prior_12m_orders {
    type: number
    sql: ${TABLE}.prior_12m_orders ;;
  }

  dimension: order_segmentation {
    type: string
    sql: ${TABLE}.order_segmentation ;;
  }

  measure: total_orders {
    type: count
    drill_fields: [current_order_id, customers_id, current_order_date]
  }
}

