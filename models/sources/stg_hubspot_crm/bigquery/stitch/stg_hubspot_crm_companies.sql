{% if target.type == "bigquery" %}
    {% if var("crm_warehouse_company_sources") %}
        {% if "hubspot_crm" in var("crm_warehouse_company_sources") %}
            {% if var("stg_hubspot_crm_etl") == "stitch" %}

                with
                    source as (
                        {{
                            filter_stitch_relation(
                                relation=source("stitch_hubspot_crm", "companies"),
                                unique_column="companyid",
                            )
                        }}
                    ),
                    renamed as (
                        select
                            concat(
                                '{{ var(' stg_hubspot_crm_id - prefix ') }}', companyid
                            ) as company_id,
                            replace(
                                replace(
                                    replace(properties.name.value, 'Limited', ''),
                                    'ltd',
                                    ''
                                ),
                                ', Inc.',
                                ''
                            ) as company_name,
                            properties.address.value as company_address,
                            properties.address2.value as company_address2,
                            properties.city.value as company_city,
                            properties.state.value as company_state,
                            properties.country.value as company_country,
                            properties.zip.value as company_zip,
                            properties.phone.value as company_phone,
                            properties.website.value as company_website,
                            properties.industry.value as company_industry,
                            properties.linkedin_company_page.value
                            as company_linkedin_company_page,
                            properties.linkedinbio.value as company_linkedin_bio,
                            properties.twitterhandle.value as company_twitterhandle,
                            properties.description.value as company_description,
                            cast(
                                null as {{ dbt_utils.type_string() }}
                            ) as company_finance_status,
                            cast(
                                null as {{ dbt_utils.type_string() }}
                            ) as company_currency_code,
                            properties.createdate.value as company_created_date,
                            properties.hs_lastmodifieddate.value company_last_modified_date
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
