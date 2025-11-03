WITH stocks AS (
    SELECT *
    FROM {{ ref('stg_production__stocks') }}
),

products AS (
    SELECT
        product_id,
        product_name,
        brand_id,
        category_id,
        model_year,
        list_price
    FROM {{ ref('stg_production__products') }}
),

store AS (
    SELECT
        store_id,
        store_name
    FROM {{ ref('stg_sales__stores') }}
),

brands AS (
    SELECT
        brand_id,
        brand_name
    FROM {{ ref('stg_production__brands') }}
),

categories AS (
    SELECT
        category_id,
        category_name
    FROM {{ ref('stg_production__categories') }}
)

SELECT
    s.store_id,
    st.store_name,
    s.product_id,
    p.product_name,
    p.model_year,
    p.list_price,
    b.brand_name,
    c.category_name,
    s.quantity AS stock_quantity
FROM stocks AS s
LEFT JOIN store AS st ON s.store_id = st.store_id
LEFT JOIN products AS p ON s.product_id = p.product_id
LEFT JOIN brands AS b ON p.brand_id = b.brand_id
LEFT JOIN categories AS c ON p.category_id = c.category_id