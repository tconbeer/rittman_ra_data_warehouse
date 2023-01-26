{% if var("subscriptions_warehouse_sources") %}

{{ config(alias="subscription_billing_fact") }}

with
    subscriptions as (
        select {{ dbt_utils.star(from=ref("wh_subscriptions_fact")) }}
        from {{ ref("wh_subscriptions_fact") }}
    ),
    customers as (
        select {{ dbt_utils.star(from=ref("wh_customers_dim")) }}
        from {{ ref("wh_customers_dim") }}
    ),
    subscription_billing as (
        select {{ dbt_utils.star(from=ref("int_subscription_billing")) }}
        from {{ ref("int_subscription_billing") }}
    )
select generate_uuid() as subscription_billing_pk, c.customer_pk, s.subscription_pk, b.*
from subscription_billing b
join customers c on b.customer_id = c.customer_alternative_id
join subscriptions s on b.subscription_id = s.subscription_id

{% else %} {{ config(enabled=false) }}

{% endif %}
