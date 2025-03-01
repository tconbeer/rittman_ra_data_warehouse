{{ config(enabled=target.type == "bigquery") }}
{% if var("projects_warehouse_delivery_sources") %}
    {% if "asana_projects" in var("projects_warehouse_delivery_sources") %}

        with
            source as (
                {{
                    filter_stitch_relation(
                        relation=var("stg_asana_projects_stitch_projects_table"),
                        unique_column="gid",
                    )
                }}
            ),

            renamed as (
                select
                    concat(
                        '{{ var(' stg_asana_projects_id - prefix ') }}', gid
                    ) as project_id,
                    concat(
                        '{{ var(' stg_asana_projects_id - prefix ') }}', owner.gid
                    ) as lead_user_id,
                    concat(
                        '{{ var(' stg_asana_projects_id - prefix ') }}', workspace.gid
                    ) as company_id,
                    name as project_name,
                    current_status as project_status,
                    notes as project_notes,
                    cast(null as {{ dbt_utils.type_string() }}) as project_type,
                    cast(
                        null as {{ dbt_utils.type_string() }}
                    ) as project_category_description,
                    cast(
                        null as {{ dbt_utils.type_string() }}
                    ) as project_category_name,
                    created_at as project_created_at_ts,
                    modified_at as project_modified_at_ts,
                from source

            )
        select *
        from renamed

    {% else %} {{ config(enabled=false) }}
    {% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
