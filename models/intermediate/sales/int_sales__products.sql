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
FROM {{ ref('stg_sales__order_items') }} oi
LEFT JOIN {{ ref('stg_production__products') }} p ON p.product_id = oi.product_id
LEFT JOIN {{ ref('stg_production__brands') }} b ON b.brand_id = p.brand_id
LEFT JOIN {{ ref('stg_production__categories') }} cat ON cat.category_id = p.category_id
LEFT JOIN {{ ref('stg_sales__orders') }} o ON o.order_id = oi.order_id