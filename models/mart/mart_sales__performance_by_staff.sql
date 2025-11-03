WITH orders AS (
    SELECT
        order_id,
        staff_id,
        staff_name,
        order_status
    FROM {{ ref('int_sales__orders_with_customers') }}
),

order_items AS (
    SELECT
        order_id,
        total_amount
    FROM {{ ref('int_sales__order_items_with_products') }}
)

SELECT
    o.staff_id,
    o.staff_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.total_amount) AS total_sales,
    AVG(oi.total_amount) AS avg_order_value
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY 1,2