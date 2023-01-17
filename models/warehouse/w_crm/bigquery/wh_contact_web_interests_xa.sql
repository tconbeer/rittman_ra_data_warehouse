{{ config(enabled=target.type == "bigquery") }}
{% if var("crm_warehouse_contact_sources") and var("product_warehouse_event_sources") %}
{{ config(alias="contacts_web_interests_xa", materialized="view") }}
with
    base_scores as (
        select
            *,
            case
                when event_details like '%Attribution%' or page_url like '%attribution%'
                then 1
            end as count_attribution,
            case
                when event_details like '%RA Warehouse%' then 1
            end as count_ra_warehouse,
            case
                when event_details like '%Pricing%' or page_url like '%pricing%' then 1
            end as count_pricing,
            case
                when event_details like '%Looker%' or page_url like '%looker%' then 1
            end as count_looker,
            case
                when event_details like '%Oracle Autonomous Data Warehouse Cloud%'
                then 1
            end as count_oawc,
            case
                when event_details like '%dbt%' or page_url like '%dbt%' then 1
            end as count_dbt,
            case
                when event_details like '%Customer Journey%' then 1
            end as count_customer_journey,
            case
                when
                    event_details like '%Data Centralization%'
                    or event_details like '%Data Centralisation%'
                    or page_url like '%central%'
                then 1
            end as count_data_centralisation,
            case
                when event_details like '%Ad Spend%' then 1
            end as count_marketing_analytics,
            case
                when event_details like '%Segment%' or page_url like '%segment%' then 1
            end as count_segment,
            case
                when
                    event_details like '%Modern BI Stack%'
                    or event_details like '%Modern Data Stack%'
                then 1
            end as count_modern_data_stack,
            case when page_url like '%/blog%' then 1 end as count_blog,
            case when page_url like '%/podcast%' then 1 end as count_podcast,
            case when page_url like '%/highgrowth%' then 1 end as count_startup,
            case
                when event_details like '%Marketing Automation%' then 1
            end as count_martech,
            case
                when event_details like '%Personas%' or page_url like '%personas%'
                then 1
            end as count_personas,
            case
                when
                    event_details like '%Google BigQuery%' or page_url like '%bigquery%'
                then 1
            end as count_bigquery,
            case
                when
                    event_details like '%Data Teams%'
                    or event_details like '%Data Strategy%'
                then 1
            end as count_data_teams,
            case when page_url like '%/customer%' then 1 end as count_casestudy,
            case when event_type like '%Button%' then 1 end as count_button_pressed,
            case
                when event_type = 'Page View'
                then 1
                when event_type in ('Clicked Link', 'Pricing View')
                then 2
                when event_type = 'Email Link Clicked'
                then 3
                when event_type like '%Button%'
                then 4
                else 0
            end as multiplier
        from {{ ref("wh_contact_web_event_history") }}
    ),
    weighted_scores as (
        select
            contact_pk,
            count_blog,
            count_podcast,
            count_pricing,
            count_casestudy,
            count_attribution * multiplier as weighted_count_attribution,
            count_ra_warehouse * multiplier as weighted_count_ra_warehouse,
            count_looker * multiplier as weighted_count_looker,
            count_oawc * multiplier as weighted_count_oawc,
            count_dbt * multiplier as weighted_count_dbt,
            count_customer_journey * multiplier as weighted_count_customer_journey,
            count_data_centralisation
            * multiplier as weighted_count_data_centralisation,
            count_marketing_analytics
            * multiplier as weighted_count_marketing_analytics,
            count_segment * multiplier as weighted_count_segment,
            count_modern_data_stack * multiplier as weighted_count_modern_data_stack,
            count_startup * multiplier as weighted_count_startup,
            count_martech * multiplier as weighted_count_martech,
            count_personas * multiplier as weighted_count_personas,
            count_bigquery * multiplier as weighted_count_bigquery,
            count_data_teams * multiplier as weighted_count_data_teams
        from base_scores
    ),
    total_scores as (
        select
            contact_pk,
            coalesce(sum(count_pricing), 0) as pricing_views,
            coalesce(sum(count_blog), 0) as blog_views,
            coalesce(sum(count_podcast), 0) as podcast_views,
            coalesce(sum(weighted_count_attribution), 0) as attribution_interest,
            coalesce(sum(weighted_count_ra_warehouse), 0) as ra_warehouse_interest,
            coalesce(sum(weighted_count_looker), 0) as looker_interest,
            coalesce(sum(weighted_count_oawc), 0) as oawc_interest,
            coalesce(sum(weighted_count_dbt), 0) as dbt_interest,
            coalesce(
                sum(weighted_count_customer_journey), 0
            ) as customer_journey_interest,
            coalesce(
                sum(weighted_count_data_centralisation), 0
            ) as data_centralisation_interest,
            coalesce(
                sum(weighted_count_marketing_analytics), 0
            ) as marketing_analytics_interest,
            coalesce(sum(weighted_count_segment), 0) as segment_interest,
            coalesce(
                sum(weighted_count_modern_data_stack), 0
            ) as modern_data_stack_interest,
            coalesce(sum(weighted_count_startup), 0) as startup_interest,
            coalesce(sum(weighted_count_martech), 0) as martech_interest,
            coalesce(sum(weighted_count_personas), 0) as personas_interest,
            coalesce(sum(weighted_count_bigquery), 0) as bigquery_interest,
            coalesce(sum(weighted_count_data_teams), 0) as data_teams_interest,
            coalesce(sum(count_casestudy), 0) as casestudy_views
        from weighted_scores
        group by 1
    ),
    last_visit as (
        select contact_pk, event_ts, page_title
        from
            (
                select
                    contact_pk,
                    event_ts,
                    page_title,
                    row_number() over (
                        partition by contact_pk order by event_ts desc
                    ) as visit_seq_desc
                from {{ ref("wh_contact_web_event_history") }}
                where event_type = 'Page View'
            )
        where visit_seq_desc = 1
    ),
    visits_last_90_days as (
        select contact_pk, count(*) as visits_l90_days
        from {{ ref("wh_contact_web_event_history") }}
        where
            event_ts >= timestamp_sub(current_timestamp, interval 90 day)
            and event_type = 'Page View'
        group by 1
    )
select
    t.*,
    l.event_ts as last_visit_ts,
    l.page_title as last_page_title,
    coalesce(v.visits_l90_days) as visits_l90_days
from total_scores t
left join last_visit l on t.contact_pk = l.contact_pk
left join visits_last_90_days v on t.contact_pk = v.contact_pk
{% else %} {{ config(enabled=false) }}
{% endif %}
