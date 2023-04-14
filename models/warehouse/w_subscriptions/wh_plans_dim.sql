{% if var("subscriptions_warehouse_sources") %}

    {{ config(alias="plans_dim") }}

    with plans as (select * from {{ ref("int_plans") }})
    select generate_uuid() as plan_pk, p.*
    from plans p

{% else %} {{ config(enabled=false) }}

{% endif %}
