{% if var("projects_warehouse_delivery_sources") %}

select
    'Unassigned' as user_id,
    'Unassigned' as user_name,
    'Unassigned' as user_email,
    false as contact_is_contractor,
    false as contact_is_staff,
    0 as contact_weekly_capacity,
    cast(null as string) as user_phone,
    0 as contact_default_hourly_rate,
    0 as contact_cost_rate,
    false as contact_is_active,
    cast(null as timestamp) as user_created_ts,
    cast(null as timestamp) as user_last_modified_ts

{% else %} {{ config(enabled=false) }}
{% endif %}
