{{ config(
    materialized='table',
) }}

SELECT *
FROM `Assignment.Orders`
