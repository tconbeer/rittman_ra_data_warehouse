{{ config(enabled=target.type == "bigquery") }}

{% if var("crm_warehouse_contact_sources") %}
{% if "xero_accounting" in var("crm_warehouse_contact_sources") %}
{% if var("stg_xero_accounting_etl") == "fivetran" %}
with
    source as (select * from {{ source("fivetran_xero_accounting", "contact") }}),
    addresses as (
        select
            contact_id,
            address_type,
            address_line_1,
            address_line_2,
            address_line_3,
            address_line_4,
            city,
            region,
            country,
            postal_code
        from {{ source("fivetran_xero_accounting", "contact_address") }}
    ),
    renamed as (
        select
            concat(
                '{{ var(' stg_xero_accounting_id - prefix ') }}', contacts.contact_id
            ) as contact_id,
            contacts.first_name as contact_first_name,
            contacts.last_name as contact_last_name,
            cast(null as {{ dbt_utils.type_string() }}) as contact_job_title,
            coalesce(
                concat(contacts.first_name, ' ', contacts.last_name),
                contacts.email_address
            ) as contact_name,
            contacts.email_address as contact_email,
            cast(null as {{ dbt_utils.type_string() }}) as company_phone,
            {{ fivetran_utils.string_agg("addresses.address_line_1", ",") }}
            as contact_address,
            {{ fivetran_utils.string_agg("addresses.city", ",") }} as contact_city,
            {{ fivetran_utils.string_agg("addresses..region", ",") }} as contact_state,
            {{ fivetran_utils.string_agg("addresses.country", ",") }}
            as contact_country,
            {{ fivetran_utils.string_agg("addresses.postal_code", ",") }}
            as contact_postcode_zip,
            cast(null as {{ dbt_utils.type_string() }}) as contact_company,
            cast(null as {{ dbt_utils.type_string() }}) as contact_website,
            cast(null as {{ dbt_utils.type_string() }}) as contact_company_id,
            cast(null as {{ dbt_utils.type_string() }}) as contact_owner_id,
            contacts.contact_status as contact_lifecycle_stage,
            cast(null as {{ dbt_utils.type_boolean() }}) as contact_is_contractor,
            cast(null as {{ dbt_utils.type_boolean() }}) as contact_is_staff,
            cast(null as {{ dbt_utils.type_int() }}) as contact_weekly_capacity,
            cast(null as {{ dbt_utils.type_int() }}) as contact_default_hourly_rate,
            cast(null as {{ dbt_utils.type_int() }}) as contact_cost_rate,
            false as contact_is_active,
            cast(null as {{ dbt_utils.type_timestamp() }}) as contact_created_date,
            cast(contacts.updated_date_utc as {{ dbt_utils.type_timestamp() }})
        from source as contacts
        left outer join
            addresses as addresses
            on contacts.contact_id = addresses.contact_id
            and addresses.address_type = 'STREET'
        where concat(contacts.first_name, ' ', contacts.last_name) is not null
        group by 1, 2, 3, 4, 5, 6, 7, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25
    )
{% endif %}
select *
from renamed
{% else %} {{ config(enabled=false) }}
{% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
