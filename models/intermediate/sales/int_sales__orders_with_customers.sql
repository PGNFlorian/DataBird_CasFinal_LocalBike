WITH orders AS (
    SELECT
        order_id,
        customer_id,
        staff_id,
        store_id,
        order_status,
        order_date,
        required_date,
        shipped_date
    FROM {{ ref('stg_sales__orders') }}
),

customers AS (
    SELECT
        customer_id,
        CONCAT(first_name, ' ', last_name) AS customer_name,
        city AS customer_city,
        state AS customer_state
    FROM {{ ref('stg_sales__customers') }}
),

staffs AS (
    SELECT
        staff_id,
        CONCAT(first_name, ' ', last_name) AS staff_name,
        store_id AS staff_store_id
    FROM {{ ref('stg_sales__staffs') }}
),

stores AS (
    SELECT
        store_id,
        store_name,
        city AS store_city,
        state AS store_state
    FROM {{ ref('stg_sales__stores') }}
)

SELECT
    o.order_id,
    o.order_date,
    o.required_date,
    o.shipped_date,
    o.order_status,
    o.customer_id,
    c.customer_name,
    c.customer_city,
    c.customer_state,
    o.staff_id,
    s.staff_name,
    o.store_id,
    st.store_name,
    st.store_city,
    st.store_state
FROM orders AS o
LEFT JOIN customers AS c ON o.customer_id = c.customer_id
LEFT JOIN staffs AS s ON o.staff_id = s.staff_id
LEFT JOIN stores AS st ON o.store_id = st.store_id