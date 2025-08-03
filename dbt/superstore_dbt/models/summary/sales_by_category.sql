-- depends_on: {{ ref('dim_products') }}
{{ config(materialized='view') }}

select 
p.category,
sum(f.sales) as total_sales,
sum(f.profit) as total_profit
from {{ ref('fact_orders') }} f
join {{ ref('dim_products') }} p
  on f.product_key = p.product_key
group by 1
order by 2 desc