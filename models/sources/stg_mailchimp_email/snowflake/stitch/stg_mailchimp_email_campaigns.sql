{{ config(enabled=target.type == "snowflake") }}
{% if var("marketing_warehouse_email_event_sources") %}
    {% if "mailchimp_email" in var("marketing_warehouse_email_event_sources") %}

        with
            source as (
                select
                    id,
                    concat('mailchimp-', id) as ad_campaign_id,
                    settings.subject_line as ad_campaign_name,
                    status as ad_campaign_status,
                    cast(null as {{ dbt_utils.type_string() }}) as campaign_buying_type,
                    content_type as campaign_content_type,
                    {{ dbt_utils.date_trunc("DAY", "send_time") }}
                    as ad_campaign_start_date,
                    {{ dbt_utils.date_trunc("DAY", "send_time") }}
                    as ad_campaign_end_date,
                    'Mailchimp' as ad_network
                from `ra-development.stitch_mailchimp.campaigns`
                group by 1, 2, 3, 4, 5, 6, 7, 8
            ),
            renamed as (
                select
                    ad_campaign_id,
                    ad_campaign_name,
                    ad_campaign_status,
                    campaign_buying_type,
                    ad_campaign_start_date,
                    ad_campaign_end_date,
                    ad_network
                from source
            )
        select *
        from renamed

    {% else %} {{ config(enabled=false) }}
    {% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
