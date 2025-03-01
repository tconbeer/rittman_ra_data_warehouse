{% if target.type == "bigquery" %}
    {% if var("crm_warehouse_company_sources") %}
        {% if "hubspot_crm" in var("crm_warehouse_company_sources") %}
            {% if var("stg_hubspot_crm_etl") == "fivetran" %}

                with
                    source as (

                        select * from {{ source("fivetran_hubspot_crm", "companies") }}
                    ),
                    renamed as (
                        select
                            concat(
                                '{{ var(' stg_hubspot_crm_id - prefix ') }}', id
                            ) as company_id,
                            replace(
                                replace(
                                    replace(property_name, 'Limited', ''), 'ltd', ''
                                ),
                                ', Inc.',
                                ''
                            ) as company_name,
                            property_address as company_address,
                            property_address_2 as company_address2,
                            property_city as company_city,
                            property_state as company_state,
                            property_country as company_country,
                            property_zip as company_zip,
                            property_phone as company_phone,
                            property_website as company_website,
                            property_industry as company_industry,
                            property_linkedin_company_page
                            as company_linkedin_company_page,
                            property_linkedinbio as company_linkedin_bio,
                            property_twitterhandle as company_twitterhandle,
                            property_description as company_description,
                            cast(null as string) as company_finance_status,
                            cast(
                                null as {{ dbt_utils.type_string() }}
                            ) as company_currency_code,
                            property_createdate as company_created_date,
                            property_hs_lastmodifieddate company_last_modified_date
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
{% else %} {{ config(enabled=false) }}
{% endif %}
