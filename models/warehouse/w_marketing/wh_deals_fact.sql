{% if var("marketing_warehouse_deal_sources") and var(
    "crm_warehouse_company_sources"
) %}

    {{ config(alias="deals_fact", unique_key="deal_id") }}

    {% if target.type == "bigquery" %}

        with companies_dim as (select * from {{ ref("wh_companies_dim") }})
    {% elif target.type == "snowflake" %}

        with
            companies_dim as (
                select c.company_pk, cf.value::string as company_id
                from
                    {{ ref("wh_companies_dim") }} c,
                    table(flatten(c.all_company_ids)) cf
            )
    {% else %}
        {{
            exceptions.raise_compiler_error(
                target.type ~ " not supported in this project"
            )
        }}
    {% endif %}

    select {{ dbt_utils.surrogate_key(["deal_id"]) }} as deal_pk, c.company_pk, d.*
    from {{ ref("int_deals") }} d
    {% if target.type == "bigquery" %}
        join companies_dim c on d.company_id in unnest(c.all_company_ids)
    {% elif target.type == "snowflake" %}
        join companies_dim c on d.company_id = c.company_id
        {% else %}
            {{
                exceptions.raise_compiler_error(
                    target.type ~ " not supported in this project"
                )
            }}
        {% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
