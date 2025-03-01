{{ config(enabled=target.type == "bigquery") }}
{% if var("product_warehouse_usage_sources") %}
    {% if "bigquery_usage" in var("product_warehouse_usage_sources") %}

        with
            source as (
                select *
                from
                    {{
                        source(
                            "bigquery_usage_product_usage", "cloudaudit_data_access"
                        )
                    }}
            ),
            renamed as (
                select
                    concat(
                        '{{ var(' stg_bigquery_usage_id - prefix ') }}',
                        resource.labels.project_id
                    ) as company_id,
                    resource.labels.project_id as company_name,
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
                    cast(
                        null as {{ dbt_utils.type_string() }}
                    ) as company_twitterhandle,
                    cast(null as {{ dbt_utils.type_string() }}) as company_description,
                    cast(
                        null as {{ dbt_utils.type_string() }}
                    ) as company_finance_status,
                    cast(
                        null as {{ dbt_utils.type_string() }}
                    ) as company_currency_code,
                    cast(null as timestamp) as company_created_date,
                    cast(null as timestamp) as company_last_modified_date
                from source {{ dbt_utils.group_by(18) }}
            )
        select *
        from renamed

    {% else %} {{ config(enabled=false) }}
    {% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
