{% if var("finance_warehouse_payment_sources") %}

{{ config(unique_key="payment_pk", alias="payments_fact") }}

with
    payments as (select * from {{ ref("int_payments") }}),
    companies_dim as (select * from {{ ref("wh_companies_dim") }}),
    currencies_dim as (select * from {{ ref("wh_currencies_dim") }})
select {{ dbt_utils.surrogate_key(["payment_id"]) }} as payment_pk, p.*
from payments p

{% else %} {{ config(enabled=false) }}
{% endif %}
