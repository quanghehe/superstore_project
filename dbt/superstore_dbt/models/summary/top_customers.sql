-- depends_on: {{ ref('dim_customers') }}
{{ config(materialized='view') }}

select
  c.customer_name,
  c.segment,
  sum(f.profit) as total_profit
from {{ ref('fact_orders') }} f
join {{ ref('dim_customers') }} c
  on f.customer_key = c.customer_key
group by 1, 2
order by total_profit desc
limit 10