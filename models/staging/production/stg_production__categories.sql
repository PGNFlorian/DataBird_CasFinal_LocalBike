SELECT
  category_id,
  category_name
FROM {{ source('production', 'categories') }}