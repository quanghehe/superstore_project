-- depends_on: {{ ref('fact_orders') }}
{{ config(materialized='view') }}

select
  date_trunc('month', order_date::date) as month,
  sum(sales) as total_sales,
  sum(profit) as total_profit,
  sum(quantity) as total_quantity
from {{ ref('fact_orders') }}
group by 1
order by 1