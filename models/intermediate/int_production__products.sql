WITH products AS (
    SELECT
        p.product_id,
        p.product_name,
        b.brand_name,
        c.category_name
    FROM {{ ref('stg_production__products') }} p
    LEFT JOIN {{ ref('stg_production__brands') }} b
        ON p.brand_id = b.brand_id
    LEFT JOIN {{ ref('stg_production__categories') }} c
        ON p.category_id = c.category_id
),

stock AS (
    SELECT
        store_id,
        product_id,
        quantity
    FROM {{ ref('stg_production__stocks') }}
)

SELECT
    s.store_id || '|' || p.product_id as store_product_id,
    p.product_id,
    p.product_name,
    p.brand_name,
    p.category_name,
    s.store_id,
    s.quantity
FROM products p
JOIN stock s
    ON s.product_id = p.product_id