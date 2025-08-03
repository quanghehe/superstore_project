{{ config(
    materialized='table'
) }}

select 
    o.order_id,
    c.customer_key,      
    p.product_key,        
    o.ship_mode,
    o.order_date,
    o.ship_date,
    o.sales,
    o.quantity,
    o.discount,
    o.profit
from stg_order o
left join dim_customers c
    on o.customer_id = c.customer_id
left join dim_products p
    on o.product_id = p.product_id
