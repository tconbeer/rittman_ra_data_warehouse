{% if var("ecommerce_warehouse_product_sources") %}

    {{ config(unique_key="product_pk", alias="products_dim") }}

    with products as (select * from {{ ref("int_products") }} o)
    select {{ dbt_utils.surrogate_key(["product_id"]) }} as product_pk, *
    from products

{% else %} {{ config(enabled=false) }}
{% endif %}
