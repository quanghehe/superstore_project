-- depends_on: {{ ref('stg_products') }}
{{ config(
    materialized='table',
    unique_key='product_key'
) }}

select
  row_number() over(order by product_id) as product_key,
  product_id,
  product_name,
  category,
  sub_category
from {{ ref('stg_products') }}