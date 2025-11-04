SELECT
    o.store_id,
    s.store_name,
    o.staff_id,
    st.first_name || ' ' || st.last_name AS staff_name,
    EXTRACT(YEAR FROM o.order_date) AS order_year,
    EXTRACT(MONTH FROM o.order_date) AS order_month,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.total) AS total_sales,
    AVG(oi.total) AS avg_order_value
FROM {{ ref('int_sales__orders') }} o
JOIN {{ ref('int_sales__products') }} oi
  ON o.order_id = oi.order_id
JOIN {{ ref('stg_sales__stores') }} s
  ON o.store_id = s.store_id
JOIN {{ ref('stg_sales__staffs') }} st
  ON o.staff_id = st.staff_id
GROUP BY 1,2,3,4,5,6