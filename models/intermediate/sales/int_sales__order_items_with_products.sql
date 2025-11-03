WITH order_items AS (
    SELECT
        order_id,
        item_id,
        product_id,
        quantity,
        list_price,
        discount
    FROM {{ ref('stg_sales__order_items') }}
),

products AS (
    SELECT
        product_id,
        product_name,
        brand_id,
        category_id,
        list_price AS product_price
    FROM {{ ref('stg_production__products') }}
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
    oi.order_id,
    oi.item_id,
    oi.product_id,
    p.product_name,
    b.brand_name,
    c.category_name,
    oi.quantity,
    oi.list_price,
    oi.discount,
    (oi.quantity * oi.list_price * (1 - oi.discount)) AS total_amount
FROM order_items AS oi
LEFT JOIN products AS p ON oi.product_id = p.product_id
LEFT JOIN brands AS b ON p.brand_id = b.brand_id
LEFT JOIN categories AS c ON p.category_id = c.category_id