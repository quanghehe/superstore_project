-- depends_on: {{ ref('stg_order') }}
{{ config(
    materialized='table',
    unique_key='customer_key'
) }}

select
    row_number() over(order by customer_id) as customer_key,
    customer_id,
    customer_name,
    segment
from {{ref('stg_customers')}}