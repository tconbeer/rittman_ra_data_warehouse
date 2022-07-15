{% if var("marketing_warehouse_ad_campaign_sources") and var(
    "product_warehouse_event_sources"
) %}

{{ config(alias="ad_campaign_performance_fact") }}

with
    campaign_performance as (select * from {{ ref("int_ad_campaign_performance") }}),
    campaigns as (select * from {{ ref("wh_ad_campaigns_dim") }}),
    web_sessions as (select * from {{ ref("wh_web_sessions_fact") }}),
    campaign_performance_joined as (
        select s.ad_campaign_pk, s.utm_source, s.utm_campaign, s.utm_medium, c.*
        from campaign_performance c
        left join campaigns s on c.ad_campaign_id = s.ad_campaign_id
    ),
    segment_clicks as (
        select
            ad_campaign_pk,
            {{ dbt_utils.date_trunc("DAY", "session_start_ts") }} as campaign_date,
            count(web_sessions_pk) as total_clicks
        from web_sessions
        where ad_campaign_pk is not null {{ dbt_utils.group_by(n=2) }}
    ),
    ad_network_clicks as (
        select
            ad_campaign_pk,
            campaign_date,
            utm_source,
            utm_campaign,
            utm_medium,
            sum(total_reported_cost) as total_reported_cost,
            sum(total_reported_clicks) as total_reported_clicks,
            sum(total_reported_impressions) as total_reported_impressions
        from
            (
                select
                    {{ dbt_utils.date_trunc("DAY", "ad_campaign_serve_ts") }}
                    as campaign_date,
                    ad_campaign_pk,
                    utm_source,
                    utm_campaign,
                    utm_medium,
                    ad_campaign_total_cost as total_reported_cost,
                    ad_campaign_total_clicks as total_reported_clicks,
                    ad_campaign_total_impressions as total_reported_impressions,
                from campaign_performance_joined
            )
            {{ dbt_utils.group_by(n=5) }}
    ),
    joined as (
        select
            a.*,
            coalesce(s.total_clicks, 0) as total_clicks,
            {{ safe_divide("s.total_clicks", "A.total_reported_clicks") }}
            as actual_vs_reported_clicks_pct,
            {{ safe_divide("a.total_reported_cost", "a.total_reported_clicks") }}
            as reported_cpc,
            {{ safe_divide("a.total_reported_cost", "s.total_clicks") }} as actual_cpc,
            {{ safe_divide("a.total_reported_clicks", "a.total_reported_impressions") }}
            as reported_ctr,
            {{ safe_divide("s.total_clicks", "a.total_reported_impressions") }}
            as actual_ctr,
            {{
                safe_divide(
                    "a.total_reported_cost*1000", "a.total_reported_impressions"
                )
            }} as reported_cpm
        from ad_network_clicks a
        left outer join
            segment_clicks s
            on s.ad_campaign_pk = a.ad_campaign_pk
            and s.campaign_date = a.campaign_date
    )
select
    {{ dbt_utils.surrogate_key(["ad_campaign_pk", "campaign_date"]) }}
    as ad_campaign_performance_pk,
    campaign_date,
    ad_campaign_pk,
    total_clicks,
    total_reported_clicks,
    actual_vs_reported_clicks_pct,
    total_reported_cost,
    reported_cpc,
    reported_ctr,
    actual_ctr,
    total_reported_impressions,
    reported_cpm
from joined
where trim(utm_campaign) is not null

{% else %} {{ config(enabled=false) }}

{% endif %}
