{% if var("finance_warehouse_invoice_sources") %}
{{ config(unique_key="account_pk", alias="chart_of_accounts_dim") }}

with chart_of_accounts as (select * from {{ ref("int_chart_of_accounts") }})

select {{ dbt_utils.surrogate_key(["account_id"]) }} as account_pk, *
from chart_of_accounts
{% else %} {{ config(enabled=false) }}
{% endif %}
