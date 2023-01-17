{{ config(enabled=target.type == "bigquery") }}
{% if var("crm_warehouse_company_sources") %}
{% if "asana_projects" in var("crm_warehouse_company_sources") %}

with
    source as (
        {{
            filter_stitch_relation(
                relation=var("stg_asana_projects_stitch_workspaces_table"),
                unique_column="gid",
            )
        }}
    ),
    renamed as (
        select
            concat('{{ var(' stg_asana_projects_id - prefix ') }}', gid) as company_id,
            name as company_name,
            cast(null as {{ dbt_utils.type_string() }}) as company_address,
            cast(null as {{ dbt_utils.type_string() }}) as company_address2,
            cast(null as {{ dbt_utils.type_string() }}) as company_city,
            cast(null as {{ dbt_utils.type_string() }}) as company_state,
            cast(null as {{ dbt_utils.type_string() }}) as company_country,
            cast(null as {{ dbt_utils.type_string() }}) as company_zip,
            cast(null as {{ dbt_utils.type_string() }}) as company_phone,
            cast(null as {{ dbt_utils.type_string() }}) as company_website,
            cast(null as {{ dbt_utils.type_string() }}) as company_industry,
            cast(
                null as {{ dbt_utils.type_string() }}
            ) as company_linkedin_company_page,
            cast(null as {{ dbt_utils.type_string() }}) as company_linkedin_bio,
            cast(null as {{ dbt_utils.type_string() }}) as company_twitterhandle,
            cast(null as {{ dbt_utils.type_string() }}) as company_description,
            cast(null as {{ dbt_utils.type_string() }}) as company_finance_status,
            cast(null as {{ dbt_utils.type_string() }}) as company_currency_code,
            cast(null as timestamp) as company_created_date,
            cast(null as timestamp) as company_last_modified_date
        from source
        where name != 'My Company'
    )
select *
from renamed

{% else %} {{ config(enabled=false) }}
{% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
