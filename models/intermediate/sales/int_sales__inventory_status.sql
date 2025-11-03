SELECT
  s.store_id,
  st.store_name,
  p.product_id,
  p.product_name,
  c.category_name,
  b.brand_name,
  stq.quantity AS stock_quantity
FROM {{ ref('stg_stocks') }} stq
LEFT JOIN {{ ref('stg_stores') }} st USING (store_id)
LEFT JOIN {{ ref('stg_products') }} p USING (product_id)
LEFT JOIN {{ ref('stg_categories') }} c USING (category_id)
LEFT JOIN {{ ref('stg_brands') }} b USING (brand_id)