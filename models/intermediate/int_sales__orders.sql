WITH orders AS (
    SELECT
        order_id,
        customer_id,
        staff_id,
        store_id,
        order_date,
        required_date,
        shipped_date,
        order_status
    FROM {{ ref('stg_sales__orders') }}
),

customers AS (
    SELECT
        customer_id,
        first_name || ' ' || last_name AS customer_name
    FROM {{ ref('stg_sales__customers') }}
),

staffs AS (
    SELECT
        staff_id,
        first_name || ' ' || last_name AS staff_name
    FROM {{ ref('stg_sales__staffs') }}
),

stores AS (
    SELECT
        store_id,
        store_name
    FROM {{ ref('stg_sales__stores') }}
)

SELECT
    o.order_id,
    o.order_date,
    o.required_date,
    o.shipped_date,
    o.order_status,
    o.store_id,
    s.store_name,
    o.staff_id,
    st.staff_name,
    o.customer_id,
    c.customer_name
FROM orders o
LEFT JOIN customers c
    ON o.customer_id = c.customer_id
LEFT JOIN staffs st
    ON o.staff_id = st.staff_id
LEFT JOIN stores s
    ON o.store_id = s.store_id