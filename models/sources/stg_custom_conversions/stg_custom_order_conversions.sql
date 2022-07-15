{% if target.type == "bigquery" or target.type == "snowflake" or target.type == "redshift" %}
{% if var("order_conversion_sources") %}
{% if "custom" in var("order_conversion_sources") %}

with
    source as (select * from {{ source("custom_conversions", "conversion_orders") }}),
    renamed as (
        select
            order_id as order_id,
            customer_id as user_id,
            order_ts as order_ts,
            session_id as session_id,
            checkout_id as checkout_id,
            total_revenue as total_revenue,
            currency_code as currency_code,
            utm_source as utm_source,
            utm_medium as utm_medium,
            utm_campaign as utm_campaign,
            utm_content as utm_content,
            utm_term as utm_term,
            channel as channel
        from source
    )
select *
from renamed

{% else %} {{ config(enabled=false) }}
{% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
