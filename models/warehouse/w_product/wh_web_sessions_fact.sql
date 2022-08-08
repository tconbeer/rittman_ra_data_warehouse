{% if var("product_warehouse_event_sources") %}

{{ config(alias="web_sessions_fact") }}

with
    sessions as (
        select *
        from
            (
                select
                    session_id,
                    session_start_ts,
                    session_end_ts,
                    events,
                    utm_source,
                    utm_content,
                    utm_medium,
                    utm_campaign,
                    utm_term,
                    search,
                    gclid,
                    first_page_url,
                    first_page_url_host,
                    first_page_url_path,
                    referrer_host,
                    device,
                    device_category,
                    last_page_url,
                    last_page_url_host,
                    last_page_url_path,
                    duration_in_s,
                    duration_in_s_tier,
                    referrer_medium,
                    referrer_source,
                    channel,
                    blended_user_id,
                    sum(mins_between_sessions) over (
                        partition by session_id
                    ) as mins_between_sessions,
                    is_bounced_session
                from {{ ref("int_web_events_sessions_stitched") }}
            )
            {{ dbt_utils.group_by(n=28) }}
    )
{% if var("marketing_warehouse_ad_campaign_sources") %}
    ,
    ad_campaigns as (select * from {{ ref("wh_ad_campaigns_dim") }}),
    joined as (
        select e.*, c.ad_campaign_pk
        from sessions e
        left join ad_campaigns c on e.utm_campaign = c.utm_campaign
    ),
    ordered as (
        {% if target.type == "bigquery" %}
        select {{ dbt_utils.surrogate_key(["to_hex(session_id)"]) }} as web_sessions_pk,
        {% else %}
        select {{ dbt_utils.surrogate_key(["session_id"]) }} as web_sessions_pk,
        {% endif %}
            *,
            row_number() over (
                partition by blended_user_id order by session_start_ts
            ) as user_session_number
        from joined
    )
select *
from ordered
{% else %} select * from events
{% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
