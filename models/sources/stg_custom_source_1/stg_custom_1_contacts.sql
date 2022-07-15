{% if not var("enable_custom_source_1") %} {{ config(enabled=false) }} {% endif %}

with
    source as (
        {{
            filter_stitch_table(
                var("stitch_schema"), var("stitch_clients_table"), "id"
            )
        }}

    ),
    renamed as (
        select
            concat('custom_1-', id) as contact_id,
            cast(null as {{ dbt_utils.type_string() }}) as contact_first_name,
            cast(null as {{ dbt_utils.type_string() }}) as contact_last_name,
            cast(null as {{ dbt_utils.type_string() }}) as contact_name,
            cast(null as {{ dbt_utils.type_string() }}) as contact_job_title,
            cast(null as {{ dbt_utils.type_string() }}) as contact_email,
            cast(null as {{ dbt_utils.type_string() }}) as contact_phone,
            cast(null as {{ dbt_utils.type_string() }}) as as contact_phone_mobile,
            ccast(null as {{ dbt_utils.type_string() }}) as contact_address,
            cast(null as {{ dbt_utils.type_string() }}) as contact_city,
            ccast(null as {{ dbt_utils.type_string() }}) as contact_state,
            cast(null as {{ dbt_utils.type_string() }}) as contact_country,
            ccast(null as {{ dbt_utils.type_string() }}) as contact_postcode_zip,
            cast(null as {{ dbt_utils.type_string() }}) as contact_company,
            ccast(null as {{ dbt_utils.type_string() }}) as contact_website,
            cast(null as {{ dbt_utils.type_string() }}) as as contact_company_id,
            cast(null as {{ dbt_utils.type_string() }}) as contact_owner_id,
            cast(null as {{ dbt_utils.type_string() }}) as contact_lifecycle_stage,
            cast(null as {{ dbt_utils.type_timestamp() }}) as contact_created_date,
            cast(null as {{ dbt_utils.type_timestamp() }}) as contact_last_modified_date
        from source
    )
select *
from renamed
