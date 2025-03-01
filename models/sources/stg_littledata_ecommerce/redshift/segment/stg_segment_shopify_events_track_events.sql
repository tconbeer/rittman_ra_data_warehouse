{{ config(enabled=target.type == "redshift") }}
{% if var("product_warehouse_event_sources") %}
    {% if "segment_shopify_events_track" in var("product_warehouse_event_sources") %}

        {{ config(materialized="table") }}
        with
            source as (

                select *
                from {{ var("stg_segment_shopify_events_segment_tracks_table") }}

            ),

            renamed as (

                select
                    id as event_id,
                    event as event_type,
                    received_at as event_ts,
                    event_text as event_details,
                    cast(null as {{ dbt_utils.type_string() }}) as page_title,
                    context_page_path as page_url_path,
                    replace(
                        {{ dbt_utils.get_url_host("context_page_referrer") }},
                        'www.',
                        ''
                    ) as referrer_host,
                    context_page_search as search,
                    context_page_url as page_url,
                    {{ dbt_utils.get_url_host("context_page_url") }} as page_url_host,
                    {{ dbt_utils.get_url_parameter("context_page_url", "gclid") }}
                    as gclid,
                    cast(null as {{ dbt_utils.type_string() }}) as utm_term,
                    cast(null as {{ dbt_utils.type_string() }}) as utm_content,
                    cast(null as {{ dbt_utils.type_string() }}) as utm_medium,
                    cast(null as {{ dbt_utils.type_string() }}) as utm_campaign,
                    cast(null as {{ dbt_utils.type_string() }}) as utm_source,
                    context_ip as ip,
                    anonymous_id as visitor_id,
                    user_id as user_id,
                    case
                        when lower(context_user_agent) like '%android%'
                        then 'Android'
                        else replace(split_part(context_user_agent, '(', 1), ';', '')
                    end as device,
                    '{{ var(' stg_segment_events_site ') }}' as site
                from source

            ),
            final as (

                select
                    *,
                    case
                        when device = 'iPhone'
                        then 'iPhone'
                        when device = 'Android'
                        then 'Android'
                        when device in ('iPad', 'iPod')
                        then 'Tablet'
                        when device in ('Windows', 'Macintosh', 'X11')
                        then 'Desktop'
                        else 'Uncategorized'
                    end as device_category
                from renamed

            )
        select *
        from final

    {% else %} {{ config(enabled=false) }}
    {% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
