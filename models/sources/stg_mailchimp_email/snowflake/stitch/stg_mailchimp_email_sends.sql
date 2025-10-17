{{ config(enabled=target.type == "snowflake") }}
{% if var("marketing_warehouse_email_event_sources") %}
    {% if "mailchimp_email" in var("marketing_warehouse_email_event_sources") %}

        with
            source as (
                select * from {{ var("stg_mailchimp_email_stitch_campaigns_table") }}
            ),
            renamed as (
                select
                    concat(
                        '{{ var(' stg_mailchimp_email_id - prefix ') }}', id
                    ) as send_id,
                    content_type as campaign_content_type,
                    create_time as campaign_created_at_ts,
                    emails_sent as total_campaign_emails_sent,
                    long_archive_url as campaign_archive_url,
                    concat(
                        '{{ var(' stg_mailchimp_email_id - prefix ') }}',
                        recipients.list_id
                    ) as list_id,
                    recipients.list_is_active as campaign_list_is_active,
                    recipients.list_name as list_name,
                    recipients.recipient_count as total_recipient_count,
                    report_summary.click_rate as click_rate_pct,
                    report_summary.clicks as total_clicks,
                    report_summary.open_rate as open_rate_pct,
                    report_summary.opens as total_opens,
                    report_summary.subscriber_clicks as total_subscriber_clicks,
                    report_summary.unique_opens as total_unique_opens,
                    resendable as campaign_is_resendable,
                    send_time as campaign_sent_ts,
                    settings.subject_line as campaign_subject_line,
                    settings.title as campaign_title,
                    status as campaign_status,
                    tracking.html_clicks as campaign_tracking_html_clicks,
                    tracking.opens as campaign_tracking_opens,
                    tracking.text_clicks as campaign_tracking_text_clicks
                from source
            )
        select *
        from renamed

    {% else %} {{ config(enabled=false) }}
    {% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
