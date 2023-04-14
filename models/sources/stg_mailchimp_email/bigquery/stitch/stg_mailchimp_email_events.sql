{{ config(enabled=target.type == "bigquery") }}
{% if var("marketing_warehouse_email_event_sources") %}
    {% if "mailchimp_email" in var("marketing_warehouse_email_event_sources") %}

        with
            source as (
                select *
                from
                    (
                        select
                            *,
                            max(_sdc_batched_at) over (
                                partition by
                                    list_id,
                                    campaign_id,
                                    email_id,
                                    timestamp,
                                    action,
                                    type,
                                    email_address
                                order by _sdc_batched_at
                                range
                                    between unbounded preceding and unbounded following
                            ) as max_sdc_batched_at
                        from {{ source("stitch_mailchimp_email", "email_activity") }}
                    )
                where _sdc_batched_at = max_sdc_batched_at
            ),
            joined as (
                select
                    concat(
                        '{{ var(' stg_mailchimp_email_id - prefix ') }}', list_id
                    ) as list_id,
                    concat(
                        '{{ var(' stg_mailchimp_email_id - prefix ') }}', campaign_id
                    ) as ad_campaign_id,
                    concat(
                        '{{ var(' stg_mailchimp_email_id - prefix ') }}', email_id
                    ) as contact_id,
                    timestamp as event_ts,
                    action,
                    type,
                    email_address,
                    replace(url, '[UNIQID]', email_id) as url
                from source
                union all
                select
                    s.list_id,
                    s.send_id as ad_campaign_id,
                    c.contact_id,
                    s.campaign_sent_ts as event_ts,
                    'stg_enrichment_clearbit_schema' as action,
                    null as type,
                    c.contact_email as email_address,
                    cast(null as {{ dbt_utils.type_string() }}) as url
                from {{ ref("stg_mailchimp_email_sends") }} s
                join
                    {{ ref("stg_mailchimp_email_list_members") }} m
                    on s.list_id = m.list_id
                join
                    {{ ref("stg_mailchimp_email_contacts") }} c
                    on c.contact_email = m.contact_email
            )
        select *
        from joined

    {% else %} {{ config(enabled=false) }}
    {% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
