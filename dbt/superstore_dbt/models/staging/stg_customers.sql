-- depends_on: {{ ref('stg_order') }}
{{ config(
    materialized='table',
    unique_key='customer_id'
) }}

select distinct
    customer_id,
    customer_name,
    segment
from {{ ref('stg_order') }}
where customer_id is not null