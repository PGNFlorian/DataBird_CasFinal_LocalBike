SELECT
    o.customer_name,
    o.customer_city,
    o.customer_state,
    o.order_date,
    
    ROUND(SUM(o.total),2) AS total_customer_revenue,
    SUM(o.quantity) AS total_customer_quantity_sold,
    ROUND(AVG(o.list_price),2) AS avg_customer_price,
    COUNT(DISTINCT o.order_id) AS total_customer_orders,
    ROUND(SUM(o.total - (o.quantity * o.list_price)),2) AS total_customer_margin
FROM {{ ref('int_sales__orders') }} o
GROUP BY 
    customer_name,
    customer_city,
    customer_state,
    order_date