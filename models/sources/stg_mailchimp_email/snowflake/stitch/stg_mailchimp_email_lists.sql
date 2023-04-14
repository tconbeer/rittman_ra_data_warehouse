{{ config(enabled=target.type == "snowflake") }}
{% if var("marketing_warehouse_email_list_sources") %}
    {% if "mailchimp_email" in var("marketing_warehouse_email_list_sources") %}

        with
            source as (
                {{
                    filter_stitch_relation(
                        relation=var("stg_mailchimp_email_stitch_lists_table"),
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
                    stats:avg_sub_rate::float as avg_sub_rate_pct,
                    stats:avg_unsub_rate::float as avg_unsub_rate_pct,
                    stats:campaign_count::int as total_campaigns,
                    stats:campaign_last_sent::timestamp as campaign_last_sent_ts,
                    stats:cleaned_count::int as total_cleaned,
                    stats:cleaned_count_since_send::int as total_cleaned_since_send,
                    stats:click_rate::float as click_rate_pct,
                    stats:last_sub_date::timestamp as last_sub_ts,
                    stats:last_unsub_date::timestamp as last_unsub_ts,
                    stats:member_count::int as total_members,
                    stats:member_count_since_send::int as total_members_since_send,
                    stats:merge_field_count::int as total_merge_fields,
                    stats:open_rate::float as open_rate_pct,
                    stats:target_sub_rate::float as target_sub_rate_pct,
                    stats:unsubscribe_count::int as total_unsubscribes,
                    stats:unsubscribe_count_since_send::int
                    as total_unsubscribes_since_send
                from source
            )
        select *
        from renamed

    {% else %} {{ config(enabled=false) }}
    {% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
