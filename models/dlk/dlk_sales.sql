{{ config(
    materialized='table',
) }}

SELECT *
FROM `Assignment.Sales`
