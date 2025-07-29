{{ config(
    materialized='table',
    unique_key='id'
) }}

select * from {{source('raw' , 'raw_orders')}}



