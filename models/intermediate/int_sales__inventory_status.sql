WITH stocks AS (
    SELECT
        store_id,
        product_id,
        quantity AS stock_quantity
    FROM {{ ref('stg_production__stocks') }}
),

stores AS (
    SELECT
        store_id,
        store_name
    FROM {{ ref('stg_sales__stores') }}
),

products AS (
    SELECT
        p.product_id,
        p.product_name,
        c.category_name,
        b.brand_name
    FROM {{ ref('stg_production__products') }} p
    LEFT JOIN {{ ref('stg_production__categories') }} c
        ON p.category_id = c.category_id
    LEFT JOIN {{ ref('stg_production__brands') }} b
        ON p.brand_id = b.brand_id
)

SELECT
    s.store_id,
    s.store_name,
    p.product_id,
    p.product_name,
    p.category_name,
    p.brand_name,
    st.stock_quantity
FROM stocks st
LEFT JOIN stores s
    ON st.store_id = s.store_id
LEFT JOIN products p
    ON st.product_id = p.product_id