{{ config(enabled=target.type == "redshift") }}

{% if var("product_warehouse_event_sources") %}
    {% if "heap_events_track" in var("product_warehouse_event_sources") %}
        {{ config(materialized="table") }}

        with recursive
            migrated_users(from_user_id, to_user_id, level) as (

                select from_user_id, to_user_id, 1 as level
                from {{ source("heap", "user_migrations") }}
                union all
                select u.from_user_id, u.to_user_id, level + 1
                from {{ source("heap", "user_migrations") }} u, migrated_users m
                where u.to_user_id = m.from_user_id and level < 4
            ),
            mapped_user_ids as (
                select from_user_id, to_user_id from migrated_users order by to_user_id
            ),
            source as (
                select *
                from {{ source("heap", "tracks") }}
                where time > current_date - interval '2 year'
            ),
            users as (select * from {{ source("heap", "users") }}),
            sessions as (
                select *
                from {{ source("heap", "sessions") }}
                where time > current_date - interval '2 year'
            ),
            renamed as (
                select
                    cast(a.event_id as {{ dbt_utils.type_string() }}) as event_id,
                    event_table_name as event_type,
                    a.time as event_ts,
                    cast(null as {{ dbt_utils.type_string() }}) as event_details,
                    cast(null as {{ dbt_utils.type_string() }}) as page_title,
                    cast(null as {{ dbt_utils.type_string() }}) as page_url_path,
                    replace(
                        {{ dbt_utils.get_url_host("referrer") }}, 'www.', ''
                    ) as referrer_host,
                    cast(null as {{ dbt_utils.type_string() }}) as search,
                    cast(null as {{ dbt_utils.type_string() }}) as page_url,
                    {{ dbt_utils.get_url_host("landing_page") }} as page_url_host,
                    cast(null as {{ dbt_utils.type_string() }}) as gclid,
                    s.utm_term as utm_term,
                    s.utm_content as utm_content,
                    s.utm_medium as utm_medium,
                    s.utm_campaign as utm_campaign,
                    s.utm_source as utm_source,
                    s.ip as ip,
                    cast(a.user_id as {{ dbt_utils.type_string() }}) as visitor_id,
                    u."identity" as user_id,
                    cast(null as {{ dbt_utils.type_string() }}) as device,
                    device as device_category,
                    {{ var("stg_heap_events_site") }} as site
                from source a
                join sessions s on a.session_id = s.session_id
                left join mapped_user_ids m on a.user_id = m.from_user_id
                join users u on coalesce(m.to_user_id, a.user_id) = u.user_id
                where a.event_table_name not ilike 'pageviews%'
            )
        select *
        from renamed
    {% else %} {{ config(enabled=false) }}
    {% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
