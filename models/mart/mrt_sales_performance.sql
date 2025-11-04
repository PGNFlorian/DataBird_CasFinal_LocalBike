SELECT
    oi.category_name,
    oi.brand_name,
    EXTRACT(YEAR FROM oi.order_date) AS order_year,
    EXTRACT(MONTH FROM oi.order_date) AS order_month,
    SUM(oi.total) AS total_revenue,
    SUM(oi.quantity) AS total_quantity_sold,
    AVG(oi.list_price) AS avg_price,
    COUNT(DISTINCT oi.order_id) AS total_orders
FROM {{ ref('int_sales__products') }} oi
JOIN {{ ref('int_sales__orders') }} o
  ON oi.order_id = o.order_id
GROUP BY 1,2,3,4
