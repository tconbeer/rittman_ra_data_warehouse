{{ config(enabled=target.type == "bigquery") }}
{% if var("marketing_warehouse_email_list_sources") %}
    {% if "mailchimp_email" in var("marketing_warehouse_email_list_sources") %}

        with
            source as (
                {{
                    filter_stitch_relation(
                        relation=source("stitch_mailchimp_email", "lists"),
                        unique_column="id",
                    )
                }}
            ),
            renamed as (
                select
                    concat(
                        '{{ var(' stg_mailchimp_email_id - prefix ') }}', id
                    ) as list_id,
                    name as audience_name,
                    stats.avg_sub_rate as avg_sub_rate_pct,
                    stats.avg_unsub_rate as avg_unsub_rate_pct,
                    stats.campaign_count as total_campaigns,
                    stats.campaign_last_sent as campaign_last_sent_ts,
                    stats.cleaned_count as total_cleaned,
                    stats.cleaned_count_since_send as total_cleaned_since_send,
                    stats.click_rate as click_rate_pct,
                    stats.last_sub_date as last_sub_ts,
                    stats.last_unsub_date as last_unsub_ts,
                    stats.member_count as total_members,
                    stats.member_count_since_send as total_members_since_send,
                    stats.merge_field_count as total_merge_fields,
                    stats.open_rate as open_rate_pct,
                    stats.target_sub_rate as target_sub_rate_pct,
                    stats.unsubscribe_count as total_unsubscribes,
                    stats.unsubscribe_count_since_send as total_unsubscribes_since_send
                from source
            )
        select *
        from renamed

    {% else %} {{ config(enabled=false) }}
    {% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
