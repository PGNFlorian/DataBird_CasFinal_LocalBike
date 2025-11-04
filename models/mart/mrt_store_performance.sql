SELECT
    o.store_id,
    o.store_name,
    o.staff_id,
    o.staff_name,
    EXTRACT(YEAR FROM o.order_date) AS order_year,
    EXTRACT(MONTH FROM o.order_date) AS order_month,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(oi.total),2) AS total_sales,
    ROUND(AVG(oi.total),2) AS avg_order_value
FROM {{ ref('int_sales__orders') }} o
JOIN {{ ref('int_sales__products') }} oi
  ON o.order_id = oi.order_id
GROUP BY store_id,store_name,staff_id,staff_name,order_year,order_month