WITH order_items AS (
    SELECT
        order_id,
        product_id,
        quantity,
        list_price,
        discount
    FROM {{ ref('stg_sales__order_items') }}
),

products AS (
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

orders AS (
    SELECT
        o.order_id,
        o.store_id,
        o.staff_id,
        o.customer_id,
        o.order_date
    FROM {{ ref('stg_sales__orders') }} o
)

SELECT
    oi.order_id,
    oi.product_id,
    p.product_name,
    p.brand_name,
    p.category_name,
    oi.quantity,
    oi.list_price,
    oi.discount,
    ROUND(oi.quantity * oi.list_price * (1 - oi.discount), 2) AS total,
    o.store_id,
    o.staff_id,
    o.customer_id,
    o.order_date
FROM order_items oi
LEFT JOIN products p
    ON oi.product_id = p.product_id
LEFT JOIN orders o
    ON oi.order_id = o.order_id