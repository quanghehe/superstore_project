-- depends_on: {{ ref('stg_order') }}
{{ config(
    materialized='table'
) }}

with source as (

    select distinct
        customer_id,
        customer_name,
        segment,
        country,
        city,
        state,
        postal_code,
        region
    from {{ ref('stg_order') }}
)

select * from source