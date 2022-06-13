{{ config(enabled=target.type == "bigquery") }}
{% if var("crm_warehouse_contact_sources") and var("product_warehouse_event_sources") %}
{{ config(alias="contacts_web_event_history_xa") }}

select
    c.contact_pk,
    e.blended_user_id as contact_email,
    e.web_event_pk,
    e.event_type,
    e.event_ts,
    e.event_details,
    e.page_title,
    e.page_url,
    e.ip
from {{ ref("wh_web_events_fact") }} e
join {{ ref("wh_contacts_dim") }} c on e.blended_user_id in unnest(c.all_contact_emails)
where blended_user_id like '%@%'
{% else %} {{ config(enabled=false) }}
{% endif %}
