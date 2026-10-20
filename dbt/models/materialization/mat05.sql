{{
    config(materialized = 'incremental')
}}

SELECT *
FROM {{ ref('raw_products') }}
