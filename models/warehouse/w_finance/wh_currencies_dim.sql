{% if var("finance_warehouse_invoice_sources") %}

    {{ config(unique_key="currency_pk", alias="currency_dim") }}

    with currencies as (select * from {{ ref("int_currencies") }})

    select {{ dbt_utils.surrogate_key(["currency_code"]) }} as currency_pk, *
    from currencies
{% else %} {{ config(enabled=false) }}
{% endif %}
