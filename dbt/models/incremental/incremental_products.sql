{{
    config(unique_key = 'product_id')
}}

WITH incremental_products AS (
    SELECT *
    FROM {{ ref('all_products') }}
)

SELECT *
FROM incremental_products

{% if is_incremental() %}

WHERE updated_at > (
    SELECT COALESCE(MAX(updated_at), '2025-01-01') FROM {{ this }}
    ) AND updated_at < '2025-03-01'

{% endif %}
