SELECT
    s.store_id,
    s.store_name,
    s.category_name,
    s.brand_name,
    SUM(s.stock_quantity) AS total_stock_quantity,
    COUNT(DISTINCT p.product_id) AS total_products
FROM {{ ref('int_production__stocks_enriched') }} s
JOIN {{ ref('int_production__products') }} p ON s.product_id = p.product_id
GROUP BY 1,2,3,4