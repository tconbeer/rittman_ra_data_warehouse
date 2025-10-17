{{ config(enabled=target.type == "snowflake") }}
{% if var("crm_warehouse_contact_sources") %}
    {% if "hubspot_crm" in var("crm_warehouse_contact_sources") %}

        {% if var("stg_hubspot_crm_etl") == "fivetran" %}
            with
                source as (
                    select * from {{ var("stg_hubspot_crm_fivetran_contacts_table") }}
                ),
                renamed as (
                    select
                        concat(
                            '{{ var(' stg_hubspot_crm_id - prefix ') }}',
                            canonical_vid::string
                        ) as contact_id,
                        property_firstname:value::string as contact_first_name,
                        property_lastname:value::string as contact_last_name,
                        coalesce(
                            concat(
                                property_firstname:value::string,
                                ' ',
                                property_lastname:value::string
                            ),
                            property_email:value::string
                        ) as contact_name,
                        property_jobtitle:value::string contact_job_title,
                        property_email:value::string as contact_email,
                        property_phone:value::string as contact_phone,
                        property_address:value::string contact_address,
                        property_city:value::string contact_city,
                        property_state:value::string contact_state,
                        property_country:value::string as contact_country,
                        property_zip:value::string contact_postcode_zip,
                        property_company:value::string contact_company,
                        property_website:value::string contact_website,
                        concat(
                            '{{ var(' stg_hubspot_crm_id - prefix ') }}',
                            associated_company:"company-id":value::string
                        ) as contact_company_id,
                        property_hubspot_owner_id:value::int as contact_owner_id,
                        property_lifecyclestage:value::string
                        as contact_lifecycle_stage,
                        cast(
                            null as {{ dbt_utils.type_boolean() }}
                        ) as contact_is_contractor,
                        cast(
                            null as {{ dbt_utils.type_boolean() }}
                        ) as contact_is_staff,
                        cast(
                            null as {{ dbt_utils.type_int() }}
                        ) as contact_weekly_capacity,
                        cast(
                            null as {{ dbt_utils.type_int() }}
                        ) as contact_default_hourly_rate,
                        cast(null as {{ dbt_utils.type_int() }}) as contact_cost_rate,
                        false as contact_is_active,
                        property_createdate:value::timestamp as contact_created_date,
                        property_lastmodifieddate:value::timestamp
                        as contact_last_modified_date
                    from source
                )
        {% elif var("stg_hubspot_crm_etl") == "stitch" %}
            with
                source as (
                    {{
                        filter_stitch_relation(
                            relation=var("stg_hubspot_crm_stitch_contacts_table"),
                            unique_column="canonical_vid",
                        )
                    }}

                ),
                renamed as (
                    select
                        canonical_vid::string as contact_id,
                        property_firstname:value::string as contact_first_name,
                        property_lastname:value::string as contact_last_name,
                        coalesce(
                            concat(
                                property_firstname:value::string,
                                ' ',
                                property_lastname:value::string
                            ),
                            property_email:value::string
                        ) as contact_name,
                        property_jobtitle:value::string contact_job_title,
                        property_email:value::string as contact_email,
                        property_phone:value::string as contact_phone,
                        property_address:value::string contact_address,
                        property_city:value::string contact_city,
                        property_state:value::string contact_state,
                        property_country:value::string as contact_country,
                        property_zip:value::string contact_postcode_zip,
                        property_company:value::string contact_company,
                        property_website:value::string contact_website,
                        associated_company:"company-id":value::string
                        as contact_company_id,
                        property_hubspot_owner_id:value::int as contact_owner_id,
                        property_lifecyclestage:value::string
                        as contact_lifecycle_stage,
                        cast(
                            null as {{ dbt_utils.type_boolean() }}
                        ) as contact_is_contractor,
                        cast(
                            null as {{ dbt_utils.type_boolean() }}
                        ) as contact_is_staff,
                        cast(
                            null as {{ dbt_utils.type_int() }}
                        ) as contact_weekly_capacity,
                        cast(
                            null as {{ dbt_utils.type_int() }}
                        ) as contact_default_hourly_rate,
                        cast(null as {{ dbt_utils.type_int() }}) as contact_cost_rate,
                        false as contact_is_active,
                        property_createdate:value::timestamp as contact_created_date,
                        property_lastmodifieddate:value::timestamp
                        as contact_last_modified_date
                    from source
                )
        {% endif %}
        select *
        from renamed

    {% else %} {{ config(enabled=false) }}
    {% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
