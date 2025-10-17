{% if var("ecommerce_warehouse_order_sources") %}

    {{ config(unique_key="order_pk", alias="orders_fact") }}

    with
        orders as (select * from {{ ref("int_orders") }} o),
        customers as (select * from {{ ref("wh_customers_dim") }} o)
    select {{ dbt_utils.surrogate_key(["order_id"]) }} as order_pk, c.customer_pk, o.*
    from orders o
    left join customers c on o.customer_id = c.customer_id

{% else %} {{ config(enabled=false) }}
{% endif %}
