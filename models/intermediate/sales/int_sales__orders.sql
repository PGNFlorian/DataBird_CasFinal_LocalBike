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
FROM {{ ref('stg_sales__orders') }} o
LEFT JOIN {{ ref('stg_sales__customers') }} c USING (customer_id)
LEFT JOIN {{ ref('stg_sales__staffs') }} st USING (staff_id)
LEFT JOIN {{ ref('stg_sales__stores') }} s USING (store_id)