SELECT
    o.store_id,
    o.store_name,
    oi.category_name,
    oi.brand_name,
    EXTRACT(YEAR FROM oi.order_date) AS order_year,
    EXTRACT(MONTH FROM oi.order_date) AS order_month,
    ROUND(SUM(oi.total),2) AS total_revenue,
    SUM(oi.quantity) AS total_quantity_sold,
    ROUND(AVG(oi.list_price),2) AS avg_price,
    COUNT(DISTINCT oi.order_id) AS total_orders
FROM {{ ref('int_sales__products') }} oi
JOIN {{ ref('int_sales__orders') }} o
  ON oi.order_id = o.order_id
GROUP BY 
    store_id,
    store_name,
    category_name,
    brand_name,
    order_year,
    order_month