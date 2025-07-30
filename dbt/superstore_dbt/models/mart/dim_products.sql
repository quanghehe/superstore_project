-- depends_on: {{ ref('stg_order') }}
{{ config(
    materialized='table',
    unique_key='id'
) }}

with source_product as (
    select 
        product_id,
        product_name,
        category,
        sub_category
    from {{ ref('stg_order') }}
)

select * from source_product

