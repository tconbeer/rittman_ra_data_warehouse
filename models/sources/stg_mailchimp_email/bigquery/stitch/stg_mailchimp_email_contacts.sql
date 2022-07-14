{{ config(enabled=target.type == "bigquery") }}
{% if var("crm_warehouse_contact_sources") %}
{% if "mailchimp_email" in var("crm_warehouse_contact_sources") %}

with
    source as (
        {{
            filter_stitch_relation(
                relation=source("stitch_mailchimp_email", "list_members"),
                unique_column="id",
            )
        }}
    ),
    renamed as
    (
        select
            concat('{{ var(' stg_mailchimp_email_id - prefix ') }}', id) as contact_id,
            merge_fields.fname as contact_first_name,
            merge_fields.lname as contact_last_name,
            case
                when concat(merge_fields.fname, ' ', merge_fields.lname) = ' '
                then email_address
                else concat(merge_fields.fname, ' ', merge_fields.lname)
            end as contact_name,
            cast(null as {{ dbt_utils.type_string() }}) as contact_job_title,
            email_address as contact_email,
            merge_fields.phone as contact_phone,
            merge_fields.address__re.addr1 as contact_address,
            merge_fields.address__re.city as contact_city,
            merge_fields.address__re.state as contact_state,
            merge_fields.address__re.country as contact_country,
            merge_fields.address__re.zip as contact_postcode_zip,
            cast(null as {{ dbt_utils.type_string() }}) as contact_company,
            cast(null as {{ dbt_utils.type_string() }}) as contact_website,
            cast(null as {{ dbt_utils.type_string() }}) as contact_company_id,
            cast(null as {{ dbt_utils.type_string() }}) as contact_owner_id,
            status as contact_lifecycle_stage,
            cast(null as {{ dbt_utils.type_boolean() }}) as contact_is_contractor,
            cast(null as {{ dbt_utils.type_boolean() }}) as contact_is_staff,
            cast(null as {{ dbt_utils.type_int() }}) as contact_weekly_capacity,
            cast(null as {{ dbt_utils.type_int() }}) as contact_default_hourly_rate,
            cast(null as {{ dbt_utils.type_int() }}) as contact_cost_rate,
            false as contact_is_active,
            timestamp_opt as contact_created_date,
            last_changed as contact_last_modified_date
        from source
    )
select *
from renamed

{% else %} {{ config(enabled=false) }}
{% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
