SELECT
  customer_id,
  TRIM(first_name) AS first_name,
  TRIM(last_name) AS last_name,
  email,
  phone,
  street,
  city,
  state,
  zip_code
FROM {{{{ source('sales', 'customers') }}}}
WHERE customer_id IS NOT NULL