{% if var("projects_warehouse_delivery_sources") %}
{{ config(unique_key="delivery_task_pk", alias="delivery_tasks_fact") }}


with
    tasks as (
        select {{ dbt_utils.star(from=ref("int_delivery_tasks")) }}
        from {{ ref("int_delivery_tasks") }}
    ),
    projects as (
        select {{ dbt_utils.star(from=ref("wh_delivery_projects_dim")) }}
        from {{ ref("wh_delivery_projects_dim") }}

    ),
    {% if target.type == "bigquery" %}
    contacts_dim as (
        select {{ dbt_utils.star(from=ref("wh_contacts_dim")) }}
        from {{ ref("wh_contacts_dim") }}
    )
    {% elif target.type == "snowflake" %}
    contacts_dim as (
        select c.contact_pk, cf.value::string as contact_id
        from {{ ref("wh_contacts_dim") }} c, table(flatten(c.all_contact_ids)) cf
    )
    {% else %}
    {{
        exceptions.raise_compiler_error(
            target.type ~ " not supported in this project"
        )
    }}
    {% endif %}
select
    {{ dbt_utils.surrogate_key(["task_id"]) }} as delivery_task_pk,
    p.delivery_project_pk,
    u.contact_pk,
    t.*
from tasks t
{% if target.type == "bigquery" %}
join
    contacts_dim u
    on cast(t.task_assignee_user_id as string) in unnest(u.all_contact_ids)
{% elif target.type == "snowflake" %}
join contacts_dim u on t.task_assignee_user_id::string = u.contact_id
    {% else %}
    {{
        exceptions.raise_compiler_error(
            target.type ~ " not supported in this project"
        )
    }}
    {% endif %}
left outer join projects p on t.project_id = p.project_id
{% else %} {{ config(enabled=false) }}

{% endif %}
