{% if var("product_warehouse_event_sources") %}

    {{ config(alias="web_events_fact") }}

    with
        events as (
            select {{ dbt_utils.surrogate_key(["event_id"]) }} as web_event_pk, *
            from {{ ref("int_web_events_sessionized") }}
        )
    {% if var("marketing_warehouse_ad_campaign_sources") %}
            ,
            ad_campaigns as (select * from {{ ref("wh_ad_campaigns_dim") }}),
            joined as (
                select e.*, c.ad_campaign_pk
                from events e
                left join ad_campaigns c on e.utm_campaign = c.utm_campaign
            )
        select *
        from joined
    {% else %} select * from events
    {% endif %}

{% else %} {{ config(enabled=false) }}
{% endif %}
