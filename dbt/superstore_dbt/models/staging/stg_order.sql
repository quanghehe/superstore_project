{{ config(
    materialized='table',
    unique_key='row_id'
) }}

select 
    "Row ID" as row_id,
    "Order ID" as order_id,
    "Order Date" as order_date,
    "Ship Date" as ship_date,
    "Ship Mode" as ship_mode,
    "Customer ID" as customer_id,
    "Customer Name" as customer_name,
    "Segment" as segment,
    "Country" as country,
    "City" as city,
    "State" as state,
    "Postal Code" as postal_code,
    "Region" as region,
    "Product ID" as product_id,
    "Category" as category,
    "Sub-Category" as sub_category,
    "Product Name" as product_name,
    "Sales" as sales,
    "Quantity" as quantity,
    "Discount" as discount,
    "Profit" as profit
from {{ source('raw', 'raw_orders') }}