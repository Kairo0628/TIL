{{
    config(materialized = 'ephemeral')
}}

SELECT *
FROM {{ ref('raw_products') }}
