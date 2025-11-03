SELECT
  order_id,
  customer_id,
  order_status,
  DATE(order_date) AS order_date,
  DATE(required_date) AS required_date,
  DATE(shipped_date) AS shipped_date,
  store_id,
  staff_id
FROM {{ source('sales', 'orders') }}