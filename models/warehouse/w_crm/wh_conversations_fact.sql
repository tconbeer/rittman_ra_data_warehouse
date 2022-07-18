{% if var("crm_warehouse_conversations_sources") and var(
    "crm_warehouse_companies_sources"
) %}

{{ config(alias="conversations_fact") }}


with
    companies_dim as (select * from {{ ref("wh_companies_dim") }}),
    contacts_dim as (select * from {{ ref("wh_contacts_dim") }})
select
    generate_uuid() as conversation_pk,
    p.contact_pk,
    m.* except (conversation_author_id, conversation_user_id, conversation_assignee_id)
from {{ ref("int_conversations") }} m
join contacts_dim p on m.conversation_author_id in unnest(p.all_contact_ids)

{% else %} {{ config(enabled=false) }}
{% endif %}
