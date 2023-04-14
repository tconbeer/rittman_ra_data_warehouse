{% if var("subscriptions_warehouse_sources") %}
    {{ config(alias="plan_breakout_fact") }}

    with
        plans as (select * from {{ ref("wh_plans_dim") }}),
        plan_breakouts as (select * from {{ ref("int_plan_breakout_metrics") }})
    select generate_uuid() as plan_breakout_pk, p.plan_pk, b.*
    from plan_breakouts b
    join plans p on b.plan_id = p.plan_id

{% else %} {{ config(enabled=false) }}

{% endif %}
