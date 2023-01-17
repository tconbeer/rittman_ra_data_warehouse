{% if var("ecommerce_warehouse_order_lines_sources") %}

{{ config(unique_key="order_line_pk", alias="order_lines_fact") }}

with
    order_lines as (select * from {{ ref("int_order_lines") }} o),
    products as (select * from {{ ref("wh_products_dim") }} p),
    orders as (select * from {{ ref("wh_orders_fact") }} p)
select
    {{ dbt_utils.surrogate_key(["l.order_id", "l.order_line_id"]) }} as order_line_pk,
    o.order_pk,
    p.product_pk,
    l.*
from order_lines l
left join orders o on l.order_id = o.order_id
left join products p on l.product_id = p.product_id

{% else %} {{ config(enabled=false) }}
{% endif %}
