WITH products AS (
    SELECT
        product_id,
        TRIM(product_name) AS product_name,
        brand_id,
        category_id,
        model_year,
        CAST(list_price AS FLOAT64) AS list_price
    FROM {{ ref('stg_production__products') }}
    WHERE product_id IS NOT NULL
),

brands AS (
    SELECT
        brand_id,
        TRIM(brand_name) AS brand_name
    FROM {{ ref('stg_production__brands') }}
),

categories AS (
    SELECT
        category_id,
        TRIM(category_name) AS category_name
    FROM {{ ref('stg_production__categories') }}
)

SELECT
    p.product_id,
    p.product_name,
    p.model_year,
    p.list_price,
    b.brand_id,
    b.brand_name,
    c.category_id,
    c.category_name
FROM products AS p
LEFT JOIN brands AS b ON p.brand_id = b.brand_id
LEFT JOIN categories AS c ON p.category_id = c.category_id