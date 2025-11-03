SELECT
  order_id,
  item_id,
  product_id,
  quantity,
  list_price,
  discount,
  (quantity * list_price * (1 - discount)) AS total
FROM {{ source('sales', 'order_items') }}