{% if var("subscriptions_warehouse_sources") %}
{{ config(alias="subscriptions_fact") }}


with
    subscriptions as (
        select {{ dbt_utils.star(from=ref('int_subscriptions')) }}
        from {{ ref("int_subscriptions") }}
    ),
    customers as (
        select {{ dbt_utils.star(from=ref('wh_customers_dim')) }}
        from {{ ref("wh_customers_dim") }}
    ),
    plans as (
        select {{ dbt_utils.star(from=ref('wh_plans_dim')) }}
        from {{ ref("wh_plans_dim") }}
    )
select generate_uuid() as subscription_pk, c.customer_pk, p.plan_pk, s.*
from subscriptions s
join customers c on s.customer_id = c.customer_alternative_id
join plans p on s.plan_id = p.plan_id

{% else %} {{ config(enabled=false) }}

{% endif %}
