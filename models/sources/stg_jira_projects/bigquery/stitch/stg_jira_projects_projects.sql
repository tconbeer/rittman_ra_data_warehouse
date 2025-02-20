{% if target.type == "bigquery" or target.type == "snowflake" or target.type == "redshift" %}
    {% if var("projects_warehouse_delivery_sources") %}
        {% if "jira_projects" in var("projects_warehouse_delivery_sources") %}

            with
                source as (
                    select * except (projectkeys)
                    from
                        (
                            select
                                concat(
                                    '{{ var(' stg_jira_projects_id - prefix ') }}', id
                                ) as project_id,
                                concat(
                                    '{{ var(' stg_jira_projects_id - prefix ') }}',
                                    replace(name, ' ', '_')
                                ) as company_id,
                                concat(
                                    '{{ var(' stg_jira_projects_id - prefix ') }}',
                                    lead.accountid
                                ) as lead_user_id,
                                name as project_name,
                                projectkeys,
                                projecttypekey as project_type_id,
                                cast(
                                    null as {{ dbt_utils.type_string() }}
                                ) as project_status,
                                cast(
                                    null as {{ dbt_utils.type_string() }}
                                ) as project_notes,

                                projectcategory.id as project_category_id,
                                _sdc_batched_at,
                                max(_sdc_batched_at) over (
                                    partition by id
                                    order by _sdc_batched_at
                                    range
                                        between unbounded preceding
                                        and unbounded following
                                ) as max_sdc_batched_at
                            from source('stitch_jira_projects', 'projects')
                        ),
                        unnest(projectkeys) jira_project_key
                    where _sdc_batched_at = max_sdc_batched_at
                ),
                types as (
                    select *
                    from
                        (
                            select
                                key as project_type_id,
                                formattedkey as project_type,
                                _sdc_batched_at,
                                max(_sdc_batched_at) over (
                                    partition by key
                                    order by _sdc_batched_at
                                    range
                                        between unbounded preceding
                                        and unbounded following
                                ) as max_sdc_batched_at
                            from {{ source("stitch_jira_projects", "project_types") }}
                        )
                    where _sdc_batched_at = max_sdc_batched_at
                ),
                categories as (
                    select *
                    from
                        (
                            select
                                id as project_category_id,
                                description as project_category_description,
                                name as project_category_name,
                                _sdc_batched_at,
                                max(_sdc_batched_at) over (
                                    partition by id
                                    order by _sdc_batched_at
                                    range
                                        between unbounded preceding
                                        and unbounded following
                                ) as max_sdc_batched_at
                            from
                                {{
                                    source(
                                        "stitch_jira_projects", "project_categories"
                                    )
                                }}
                        )
                    where _sdc_batched_at = max_sdc_batched_at
                )
            select
                p.project_id,
                p.lead_user_id,
                p.company_id,
                p.project_name,
                p.project_status,
                p.project_notes,
                t.project_type as project_type,
                c.project_category_description,
                c.project_category_name,
                cast(null as timestamp) as project_created_at_ts,
                cast(null as timestamp) as project_modified_at_ts

            from source p
            left outer join types t on p.project_type_id = t.project_type_id
            left outer join
                categories c on p.project_category_id = c.project_category_id

        {% else %} {{ config(enabled=false) }}
        {% endif %}
    {% else %} {{ config(enabled=false) }}
    {% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
