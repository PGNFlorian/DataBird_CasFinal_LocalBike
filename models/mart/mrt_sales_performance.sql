SELECT
    o.store_name,
    o.staff_name,
    p.category_name,
    p.brand_name,
    o.order_date,
    
    ROUND(SUM(o.total),2) AS total_revenue,
    SUM(o.quantity) AS total_quantity_sold,
    ROUND(AVG(o.list_price),2) AS avg_price,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(o.total - (o.quantity * o.list_price)),2) AS total_margin
FROM {{ ref('int_production__products') }} p
JOIN {{ ref('int_sales__orders') }} o
  ON o.store_product_id = p.store_product_id
GROUP BY 
    store_name,
    staff_name,
    category_name,
    brand_name,
    order_date