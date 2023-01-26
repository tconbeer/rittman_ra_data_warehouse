{% if var("projects_warehouse_delivery_sources") %}
{{ config(unique_key="delivery_projects_pk", alias="delivery_projects_dim") }}

with
    delivery_projects as (select * from {{ ref("int_delivery_projects") }}),
    {% if target.type == "bigquery" %}
    companies_dim as (
        select {{ dbt_utils.star(from=ref("wh_companies_dim")) }}
        from {{ ref("wh_companies_dim") }}
    )
    {% elif target.type == "snowflake" %}
    companies_dim as (
        select c.company_pk, cf.value::string as company_id
        from {{ ref("wh_companies_dim") }} c, table(flatten(c.all_company_ids)) cf
    )
    {% else %}
    {{
        exceptions.raise_compiler_error(
            target.type ~ " not supported in this project"
        )
    }}
    {% endif %}
select
    {{ dbt_utils.surrogate_key(["p.project_id"]) }} as delivery_project_pk,
    p.project_id,
    c.company_pk,
    p.project_name,
    p.project_status,
    p.project_notes,
    p.project_type as project_type,
    p.project_category_description,
    p.project_category_name,
    p.project_created_at_ts,
    p.project_modified_at_ts
from delivery_projects p
{% if target.type == "bigquery" %}
join companies_dim c on p.company_id in unnest(c.all_company_ids)
{% elif target.type == "snowflake" %}
join companies_dim c on p.company_id = c.company_id
    {% else %}
    {{
        exceptions.raise_compiler_error(
            target.type ~ " not supported in this project"
        )
    }}
    {% endif %}

{% else %} {{ config(enabled=false) }}

{% endif %}
