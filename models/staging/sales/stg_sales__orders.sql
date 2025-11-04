SELECT
  order_id,
  customer_id,
  order_status,
  CAST(order_date AS TIMESTAMP) AS order_date,
  CAST(required_date AS TIMESTAMP) AS required_date,
  CASE 
    WHEN shipped_date IN ('NULL', '') THEN NULL
    ELSE CAST(shipped_date AS TIMESTAMP)
  END AS shipped_date,
  store_id,
  staff_id
FROM {{ source('sales', 'orders') }}