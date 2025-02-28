{% if var("ecommerce_warehouse_customer_sources") %}

    {{ config(unique_key="customer_pk", alias="customer_dim") }}

    with customers as (select * from {{ ref("int_customers") }} o)
    select {{ dbt_utils.surrogate_key(["customer_id"]) }} as customer_pk, *
    from customers

{% else %} {{ config(enabled=false) }}
{% endif %}
