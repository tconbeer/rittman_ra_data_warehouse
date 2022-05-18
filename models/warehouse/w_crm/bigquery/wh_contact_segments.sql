{{ config(enabled=target.type == "bigquery") }}
{% if var("crm_warehouse_contact_sources") and var("crm_warehouse_company_sources") %}
{{ config(alias="contacts_segments_xa", materialized="view") }}
select
    email,
    name,
    case
        when visits_l90_days > 30
        then 'Highly Engaged'
        when visits_l90_days between 1 and 30
        then 'Engaged'
        else 'Historic'
    end as engagement_level,
    case when pricing_views > 0 then 'Buying' else 'Researching' end as buying_stage,
    case
        when influencer_status is null and (pricing_views > 0 or casestudy_views > 0)
        then 'Prospect'
        when influencer_status is null and (pricing_views = 0 and casestudy_views = 0)
        then 'Visitor'
        when
            influencer_status = 'Influencer' and (
                attribution_interest
                + casestudy_views
                + customer_journey_interest
                + data_centralisation_interest
                + data_teams_interest
                + dbt_interest
                + looker_interest
                + personas_interest
                + ra_warehouse_interest
                + segment_interest
            ) > 0
        then 'Engaged Influencer'
        when
            influencer_status = 'Champion' and (
                pricing_views > 0 or casestudy_views > 0
            )
        then 'Champion Prospect'
        else 'Contact'
    end as contact_segment
from {{ ref("wh_contacts_audiences_xa") }}
{% else %} {{ config(enabled=false) }}
{% endif %}
