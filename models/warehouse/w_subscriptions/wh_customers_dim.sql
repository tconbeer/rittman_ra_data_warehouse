{% if var("subscriptions_warehouse_sources") %}
    {{ config(alias="customers_dim") }}

    with customers as (select * from {{ ref("int_customers") }})
    select generate_uuid() as customer_pk, c.*
    from customers c

{% else %} {{ config(enabled=false) }}

{% endif %}
