SELECT
  o.order_id,
  o.order_date,
  o.required_date,
  o.shipped_date,
  o.order_status,
  o.store_id,
  s.store_name,
  o.staff_id,
  st.first_name || ' ' || st.last_name AS staff_name,
  o.customer_id,
  c.first_name || ' ' || c.last_name AS customer_name
FROM {{ ref('stg_sales__orders') }} AS o
LEFT JOIN {{ ref('stg_sales__customers') }} AS c
  ON o.customer_id = c.customer_id
LEFT JOIN {{ ref('stg_sales__staffs') }} AS st
  ON o.staff_id = st.staff_id
LEFT JOIN {{ ref('stg_sales__stores') }} AS s
  ON o.store_id = s.store_id