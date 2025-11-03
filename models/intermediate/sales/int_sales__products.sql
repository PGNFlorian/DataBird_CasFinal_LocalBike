SELECT
  oi.order_id,
  oi.product_id,
  p.product_name,
  b.brand_name,
  cat.category_name,
  oi.quantity,
  oi.list_price,
  oi.discount,
  oi.total,
  o.store_id,
  o.staff_id,
  o.customer_id,
  o.order_date
FROM {{ ref('stg_order_items') }} oi
LEFT JOIN {{ ref('stg_products') }} p USING (product_id)
LEFT JOIN {{ ref('stg_brands') }} b USING (brand_id)
LEFT JOIN {{ ref('stg_categories') }} cat USING (category_id)
LEFT JOIN {{ ref('stg_orders') }} o USING (order_id)