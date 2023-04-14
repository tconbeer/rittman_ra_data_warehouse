{{ config(enabled=target.type == "bigquery") }}
{% if var("crm_warehouse_contact_sources") and var("crm_warehouse_company_sources") %}
    {{ config(alias="contacts_audiences_xa", materialized="view") }}
    with
        contacts_dim as (
            select
                ct.*,
                hb.contact_id as hubspot_contact_id,
                ce.contact_email as contact_email,

                c.company_pk
            from
                (
                    select *
                    from
                        {{ ref("wh_contacts_dim") }},
                        unnest(all_contact_company_ids) as company_id
                ) ct
            join
                (
                    select *
                    from
                        {{ ref("wh_companies_dim") }} c,
                        unnest(all_company_ids) as company_id
                ) c
                on ct.company_id = c.company_id
            left join
                (
                    select contact_pk, contact_id
                    from
                        {{ ref("wh_contacts_dim") }},
                        unnest(all_contact_ids) as contact_id
                    where contact_id like '%hubspot%'
                ) hb
                on ct.contact_pk = hb.contact_pk
            left join
                (
                    select contact_pk, contact_email
                    from
                        {{ ref("wh_contacts_dim") }},
                        unnest(all_contact_emails) as contact_email
                ) ce
                on ct.contact_pk = ce.contact_pk
            where ct.company_id = c.company_id
        )
    select
        contacts_dim.contact_email as email,
        contacts_dim.contact_name as name,
        contacts_web_interests_xa.last_page_title as last_page_title,
        cast(contacts_web_interests_xa.last_visit_ts as date) as last_visit_date,
        coalesce(
            sum(contacts_web_interests_xa.attribution_interest), 0
        ) as attribution_interest,
        coalesce(sum(contacts_web_interests_xa.casestudy_views), 0) as casestudy_views,
        coalesce(
            sum(contacts_web_interests_xa.customer_journey_interest), 0
        ) as customer_journey_interest,
        coalesce(
            sum(contacts_web_interests_xa.data_centralisation_interest), 0
        ) as data_centralisation_interest,
        coalesce(
            sum(contacts_web_interests_xa.data_teams_interest), 0
        ) as data_teams_interest,
        coalesce(sum(contacts_web_interests_xa.dbt_interest), 0) as dbt_interest,
        coalesce(sum(contacts_web_interests_xa.looker_interest), 0) as looker_interest,
        coalesce(
            sum(contacts_web_interests_xa.personas_interest), 0
        ) as personas_interest,
        coalesce(sum(contacts_web_interests_xa.pricing_views), 0) as pricing_views,
        coalesce(
            sum(contacts_web_interests_xa.ra_warehouse_interest), 0
        ) as ra_warehouse_interest,
        coalesce(
            sum(contacts_web_interests_xa.segment_interest), 0
        ) as segment_interest,
        coalesce(sum(contacts_web_interests_xa.visits_l90_days), 0) as visits_l90_days
    from contacts_dim
    left join
        {{ ref("wh_contact_web_interests_xa") }} as contacts_web_interests_xa
        on contacts_dim.contact_pk = contacts_web_interests_xa.contact_pk

    group by 1, 2, 3, 4
{% else %} {{ config(enabled=false) }}
{% endif %}
