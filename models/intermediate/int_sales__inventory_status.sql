SELECT
  st.store_id,
  st.store_name,
  p.product_id,
  p.product_name,
  c.category_name,
  b.brand_name,
  stq.quantity AS stock_quantity
FROM {{ ref('stg_production__stocks') }} stq
LEFT JOIN {{ ref('stg_sales__stores') }} st ON st.store_id = stq.store_id
LEFT JOIN {{ ref('stg_production__products') }} p ON p.product_id = stq.product_id
LEFT JOIN {{ ref('stg_production__categories') }} c ON c.category_id = p.category_id
LEFT JOIN {{ ref('stg_production__brands') }} b ON b.brand_id = p.brand_id