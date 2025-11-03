WITH order_items AS (
    SELECT
        oi.order_id,
        oi.total_amount,
        p.product_id,
        p.brand_name,
        p.category_name
    FROM {{ ref('int_sales__order_items_with_products') }} oi
    JOIN {{ ref('stg_production__products') }} p ON oi.product_id = p.product_id
),

orders AS (
    SELECT
        order_id,
        store_id,
        store_name,
        staff_id,
        staff_name,
        customer_id,
        customer_name,
        order_date,
        order_status
    FROM {{ ref('int_sales__orders_with_customers') }}
)

SELECT
    o.store_id,
    o.store_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.total_amount) AS total_sales,
    AVG(oi.total_amount) AS avg_order_value,
    COUNT(DISTINCT o.customer_id) AS total_customers
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY 1,2