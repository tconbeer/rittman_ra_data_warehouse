{{ config(enabled=target.type == "snowflake") }}
{% if var("marketing_warehouse_email_event_sources") %}
    {% if "hubspot_email" in var("marketing_warehouse_email_event_sources") %}

        with
            source as (
                select *
                from
                    (
                        select
                            *,
                            max(_sdc_received_at) over (
                                partition by id, date(_sdc_received_at)
                            ) as max_sdc_received_at_for_day
                        from {{ var("stg_hubspot_email_stitch_campaigns_table") }}
                    )
                where _sdc_received_at = max_sdc_received_at_for_day
                order by id, _sdc_received_at
            ),
            renamed as (
                select
                    concat(
                        '{{ var(' stg_hubspot_email_id - prefix ') }}', id
                    ) as ad_campaign_id,
                    {{ dbt_utils.date_trunc("DAY", "_sdc_received_at::TIMESTAMP") }}
                    as ad_campaign_serve_ts,
                    coalesce(numincluded, 0) - coalesce(
                        lag(numincluded) over (
                            partition by id order by _sdc_received_at
                        ),
                        0
                    ) as ad_campaign_total_audience,
                    coalesce(counters:processed::int, 0) - coalesce(
                        lag(counters:processed::int) over (
                            partition by id order by _sdc_received_at
                        ),
                        0
                    ) as ad_campaign_emails_processed,
                    coalesce(counters:bounce::int, 0) - coalesce(
                        lag(counters:bounce::int) over (
                            partition by id order by _sdc_received_at
                        ),
                        0
                    ) as ad_campaign_bounces,
                    coalesce(counters:delivered::int, 0) - coalesce(
                        lag(counters:delivered::int) over (
                            partition by id order by _sdc_received_at
                        ),
                        0
                    ) as ad_campaign_total_emails_delivered,
                    coalesce(counters:sent::int, 0) - coalesce(
                        lag(counters:sent::int) over (
                            partition by id order by _sdc_received_at
                        ),
                        0
                    ) as ad_campaign_total_emails_sent,
                    coalesce(counters:open::int, 0) - coalesce(
                        lag(counters:open::int) over (
                            partition by id order by _sdc_received_at
                        ),
                        0
                    ) as ad_campaign_total_emails_open,
                    coalesce(counters:deferred::int, 0) - coalesce(
                        lag(counters:deferred::int) over (
                            partition by id order by _sdc_received_at
                        ),
                        0
                    ) as ad_campaign_total_emails_deferred,
                    coalesce(counters:dropped::int, 0) - coalesce(
                        lag(counters:dropped::int) over (
                            partition by id order by _sdc_received_at
                        ),
                        0
                    ) as ad_campaign_total_emails_dropped,
                    coalesce(counters:click::int, 0) - coalesce(
                        lag(counters:click::int) over (
                            partition by id order by _sdc_received_at
                        ),
                        0
                    ) as ad_campaign_total_emails_clicks,
                    coalesce(counters:unsubscribed::int, 0) - coalesce(
                        lag(counters:unsubscribed::int) over (
                            partition by id order by _sdc_received_at
                        ),
                        0
                    ) as ad_campaign_total_emails_unsubscribed,
                    'Hubspot Email' as ad_network
                from source
            )
        select *
        from renamed

    {% else %} {{ config(enabled=false) }}
    {% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
