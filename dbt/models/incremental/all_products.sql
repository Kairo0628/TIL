{{
    config(materialized = 'table')
}}

WITH all_products AS (
    SELECT *
    FROM {{ ref('raw_products') }}
)

SELECT *
FROM all_products
