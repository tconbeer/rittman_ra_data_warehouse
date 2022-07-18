{{ config(enabled=target.type == "redshift") }}

{% if var("product_warehouse_event_sources") %}
{% if "heap_events_page" in var("product_warehouse_event_sources") %}
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
        from {{ source("heap", "pages") }}
        where time > current_date - interval '2 year'
    ),
    users as (select * from {{ source("heap", "users") }}),
    renamed as (
        select
            cast(event_id as {{ dbt_utils.type_string() }}) as event_id,
            'Page View' as event_type,
            time as event_ts,
            title as event_details,
            title as page_title,
            path as page_url_path,
            replace(
                {{ dbt_utils.get_url_host("referrer") }}, 'www.', ''
            ) as referrer_host,
            query as search,
            concat(domain, path) as page_url,
            domain as page_url_host,
            cast(null as {{ dbt_utils.type_string() }}) as gclid,
            utm_term as utm_term,
            utm_content as utm_content,
            utm_medium as utm_medium,
            utm_campaign as utm_campaign,
            utm_source as utm_source,
            ip as ip,
            cast(p.user_id as {{ dbt_utils.type_string() }}) as visitor_id,
            u."identity" as user_id,
            platform as device,
            device as device_category,
            domain as site
        from source p
        left join mapped_user_ids m on p.user_id = m.from_user_id
        join users u on coalesce(m.to_user_id, p.user_id) = u.user_id
    )
select *
from renamed
{% else %} {{ config(enabled=false) }}
{% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
