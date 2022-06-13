{% if var("marketing_warehouse_ad_campaign_sources") %}

with
    ad_reporting as (
        select
            campaign_id as ad_campaign_id,
            campaign_name as ad_campaign_name,
            platform as ad_network,
            account_name as ad_account_name,
            account_id as ad_account_id,
            utm_source as utm_source,
            utm_medium as utm_medium,
            utm_campaign as utm_campaign,
            utm_content as utm_content,
            utm_term as utm_term
        from {{ ref("int_ad_reporting") }}
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    )
select *
from ad_reporting
{% else %} {{ config(enabled=false) }}


{% endif %}
