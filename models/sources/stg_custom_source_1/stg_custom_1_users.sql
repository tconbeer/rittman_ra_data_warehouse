{% if not var("enable_custom_source_1") %} {{ config(enabled=false) }} {% endif %}

with
    source as (
        {{
            filter_stitch_table(
                var("custom_source_1"), var("stitch_projects_table"), "gid"
            )
        }}

    ),
    renamed as (
        select
            concat('custom_1-', id) as user_id,
            cast(null as {{ dbt_utils.type_string() }}) as user_name,
            cast(null as {{ dbt_utils.type_string() }}) as user_email,
            cast(null as {{ dbt_utils.type_boolean() }}) as contact_is_contractor,
            cast(null as {{ dbt_utils.type_boolean() }}) as contact_is_staff,
            cast(null as numeric) as contact_weekly_capacity,
            cast(null as {{ dbt_utils.type_string() }}) as user_phone,
            cast(null as numeric) as contact_default_hourly_rate,
            cast(null as numeric) as contact_cost_rate,
            cast(null as {{ dbt_utils.type_boolean() }}) as contact_is_active,
            cast(null as {{ dbt_utils.type_timestamp() }}) as user_created_ts,
            cast(null as {{ dbt_utils.type_timestamp() }}) as user_last_modified_ts
        from source
    )
select *
from renamed
