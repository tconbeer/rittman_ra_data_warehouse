{{ config(enabled=target.type == "snowflake") }}
{% if var("marketing_warehouse_email_event_sources") %}
    {% if "hubspot_email" in var("marketing_warehouse_email_event_sources") %}

        with
            source as (
                select
                    ad_campaign_serve_ts,
                    ad_campaign_id as ad_campaign_id,
                    null as ad_campaign_budget,
                    null as ad_campaign_avg_cost,
                    null as ad_campaign_avg_time_on_site,
                    {{
                        safe_divide(
                            "ad_campaign_bounces",
                            "ad_campaign_total_emails_delivered",
                        )
                    }} as ad_campaign_bounce_rate,
                    cast(null as string) as ad_campaign_status,
                    null as ad_campaign_total_assisted_conversions,
                    ad_campaign_total_emails_clicks as ad_campaign_total_clicks,
                    null as ad_campaign_total_conversion_value,
                    null as ad_campaign_total_conversions,
                    null as ad_campaign_total_cost,
                    ad_campaign_total_emails_open
                    + ad_campaign_total_emails_clicks as ad_campaign_total_engagements,
                    ad_campaign_total_emails_open as ad_campaign_total_impressions,
                    ad_campaign_bounces
                    + ad_campaign_total_emails_unsubscribed
                    as ad_campaign_total_invalid_clicks,
                    ad_network
                from {{ ref("stg_hubspot_email_email_performance") }}
            )
        select *
        from source

    {% else %} {{ config(enabled=false) }}
    {% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
