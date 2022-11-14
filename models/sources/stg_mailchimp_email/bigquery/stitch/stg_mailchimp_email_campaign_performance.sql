{{ config(enabled=target.type == "bigquery") }}
{% if var("marketing_warehouse_email_event_sources") %}
{% if "mailchimp_email" in var("marketing_warehouse_email_event_sources") %}

with
    source as (
        select *
        from
            (
                select
                    id,
                    concat('mailchimp-', id) as ad_campaign_id,
                    content_type as campaign_content_type,
                    create_time as campaign_created_at_ts,
                    emails_sent as total_campaign_emails_sent,
                    long_archive_url as campaign_archive_url,
                    concat('mailchimp-', recipients.list_id) as list_id,
                    recipients.list_is_active as campaign_list_is_active,
                    recipients.list_name as list_name,
                    recipients.recipient_count as total_recipient_count,
                    report_summary.click_rate as click_rate_pct,
                    report_summary.clicks as total_clicks,
                    report_summary.unique_opens as total_unique_opens,
                    report_summary.open_rate as open_rate_pct,
                    report_summary.opens as total_opens,
                    report_summary.subscriber_clicks as total_subscriber_clicks,
                    resendable as campaign_is_resendable,
                    {{ dbt_utils.date_trunc("DAY", "send_time") }}
                    as ad_campaign_serve_ts,
                    settings.subject_line as campaign_subject_line,
                    settings.title as campaign_title,
                    status as campaign_status,
                    tracking.html_clicks as campaign_tracking_html_clicks,
                    tracking.opens as campaign_tracking_opens,
                    tracking.text_clicks as campaign_tracking_text_clicks,
                    _sdc_batched_at,
                    max(_sdc_batched_at) over (
                        partition by id
                        order by
                            _sdc_batched_at range
                            between unbounded preceding and unbounded following
                    ) as max_sdc_batched_at,
                from
                    {{ source("stitch_mailchimp_email", "campaigns") }}
                    {{ dbt_utils.group_by(25) }}
            )
        where _sdc_batched_at = max_sdc_batched_at
    ),
    renamed as (
        select
            ad_campaign_serve_ts,
            ad_campaign_id,
            null as ad_campaign_budget,
            null as ad_campaign_avg_cost,
            null as ad_campaign_avg_time_on_site,
            null as ad_campaign_bounce_rate,
            cast(null as {{ dbt_utils.type_string() }}) as ad_campaign_status,
            null as ad_campaign_total_assisted_conversions,
            total_clicks as ad_campaign_total_clicks,
            null as ad_campaign_total_conversion_value,
            null as ad_campaign_total_conversions,
            total_recipient_count * 0.01642 as ad_campaign_total_cost,
            total_unique_opens as ad_campaign_total_engagements,
            total_campaign_emails_sent as ad_campaign_total_impressions,
            null as ad_campaign_total_invalid_clicks,
            'Mailchimp' as ad_network
        from source
    )
select *
from renamed

{% else %} {{ config(enabled=false) }}
{% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
