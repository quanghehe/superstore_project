-- depends_on: {{ ref('stg_order') }}
{{ config(
    materialized='table',
    unique_key='product_id'
) }}

select distinct
    product_id,
    product_name,
    category,
    sub_category
from {{ ref('stg_order') }}
where product_id is not null