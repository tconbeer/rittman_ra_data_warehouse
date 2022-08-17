{% if var("subscriptions_warehouse_sources") %}

select
    d.event_id as event_id,
    d.user_id,
    max(d.event_details) as plan_id,
    max(d.event_ts) as subscribe_event_ts,
    max(p.plan_interval) as plan_interval,
    max(p.plan_name) as plan_name,
    max(p.plan_interval_count) as plan_interval_count,
    max(p.plan_amount / 100) as plan_amount,
    max(b.plan_ltv / 100) as baremetrics_predicted_ltv
from {{ ref("stg_segment_dashboard_events_events") }} d
join {{ ref("stg_stripe_subscriptions_plans") }} p on d.event_details = p.plan_id
join {{ ref("stg_baremetrics_plan_breakout") }} b on p.plan_id = b.plan_id
where
    d.event_type = 'subscribed' and date(b.plan_breakout_ts) = date(d.event_ts)
    {{ dbt_utils.group_by(n=2) }}

{% else %} {{ config(enabled=false) }}
{% endif %}
