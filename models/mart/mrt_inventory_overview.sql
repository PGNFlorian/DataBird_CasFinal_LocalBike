SELECT
    inv.store_id,
    inv.store_name,
    inv.category_name,
    inv.brand_name,
    COUNT(DISTINCT inv.product_id) AS total_products,
    SUM(inv.stock_quantity) AS total_stock_quantity,
    AVG(inv.stock_quantity) AS avg_stock_per_product,
    CURRENT_DATE() AS snapshot_date,
    SUM(CASE WHEN inv.stock_quantity = 0 THEN 1 ELSE 0 END) AS out_of_stock_products,
    SUM(CASE WHEN inv.stock_quantity < 5 THEN 1 ELSE 0 END) AS low_stock_products
FROM {{ ref('int_sales__inventory_status') }} inv
GROUP BY 1,2,3,4
ORDER BY total_stock_quantity DESC