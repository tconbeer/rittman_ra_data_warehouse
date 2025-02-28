{% if var("crm_warehouse_deal_sources") and var("crm_warehouse_contact_sources") %}

    {{ config(alias="contact_deals_fact") }}

    with
        contacts_dim as (
            select contact_pk, all_contact_ids as contact_id
            from
                {{ ref("wh_contacts_dim") }}, unnest(all_contact_ids) as all_contact_ids
        ),
        deals_fact as (select * from {{ ref("wh_deals_fact") }}),
        contact_deals as (select * from {{ ref("int_contact_deals") }})
    select
        {{ dbt_utils.surrogate_key(["contact_pk", "deal_pk"]) }} as contact_deal_pk,
        contact_pk,
        deal_pk
    from contact_deals cd
    join contacts_dim c on cd.contact_id = c.contact_id
    join deals_fact d on cd.deal_id = d.deal_id

{% else %} {{ config(enabled=false) }}
{% endif %}
