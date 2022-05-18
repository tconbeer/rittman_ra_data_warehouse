{% if not var("enable_mailchimp_email_source") or (
    not var("enable_marketing_warehouse")
) %} {{ config(enabled=false) }}
{% else %} {{ config(alias="email_events_fact") }}
{% endif %}

with
    ad_campaigns_dim as (select * from {{ ref("wh_ad_campaigns_dim") }}),
    email_lists_dim as (select * from {{ ref("wh_email_lists_dim") }}),
    contacts_dim as (select * from {{ ref("wh_contacts_dim") }}),
    email_events as (select * from {{ ref("int_email_events") }})
-- l.list_pk,
select generate_uuid() as email_event_pk, c.contact_pk, k.ad_campaign_pk, o.*
except (list_id, contact_id)
from email_events o
join contacts_dim c on o.contact_id in unnest(c.all_contact_ids)
-- LEFT JOIN email_lists_dim l
-- ON o.list_id = l.list_id
left join ad_campaigns_dim k on o.ad_campaign_id = k.ad_campaign_id
