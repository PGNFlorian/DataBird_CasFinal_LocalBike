SELECT
    p.category_name,
    p.brand_name,
    SUM(oi.total_amount) AS total_revenue,
    SUM(oi.quantity) AS total_quantity_sold,
    AVG(oi.list_price) AS avg_price,
    COUNT(DISTINCT oi.order_id) AS total_orders
FROM {{ ref('int_sales__order_items_with_products') }} oi
JOIN {{ ref('stg_production__products') }} p ON oi.product_id = p.product_id
GROUP BY 1,2