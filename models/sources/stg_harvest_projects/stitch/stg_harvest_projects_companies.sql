{% if target.type == "bigquery" or target.type == "snowflake" or target.type == "redshift" %}
{% if var("crm_warehouse_company_sources") %}
{% if "harvest_projects" in var("crm_warehouse_company_sources") %}

with
    source as (
        {{
            filter_stitch_relation(
                relation=source("stitch_harvest_projects", "companies"),
                unique_column="id",
            )
        }}
    ),
    renamed as (
        select
            concat('{{ var(' stg_harvest_projects_id - prefix ') }}', id) as company_id,
            replace(
                replace(replace(name, 'Limited', ''), 'ltd', ''), ', Inc.', ''
            ) as company_name,
            address as company_address,
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
            currency as company_currency_code,
            created_at as company_created_date,
            updated_at as company_last_modified_date
        from source
    )
select *
from renamed

{% else %} {{ config(enabled=false) }}
{% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
