{% if not var("enable_custom_source_2") %} {{ config(enabled=false) }} {% endif %}

with
    source as (select * from {{ source("custom_source_2", "s_projects") }}),
    renamed as (
        select
            concat('custom_2-', id) as timesheet_project_id,
            cast(null as {{ dbt_utils.type_string() }}) as company_id,
            cast(null as {{ dbt_utils.type_string() }}) as project_name,
            cast(null as {{ dbt_utils.type_string() }}) as project_code,
            cast(null as {{ dbt_utils.type_timestamp() }}) as project_delivery_start_ts,
            cast(null as {{ dbt_utils.type_timestamp() }}) as project_delivery_end_ts,
            cast(null as {{ dbt_utils.type_boolean() }}) as project_is_active,
            cast(null as {{ dbt_utils.type_boolean() }}) as project_is_billable,
            cast(null as numeric) as project_hourly_rate,
            cast(null as numeric) as project_cost_budget,
            cast(null as {{ dbt_utils.type_boolean() }}) as project_is_fixed_fee,
            cast(
                null as {{ dbt_utils.type_boolean() }}
            ) s as project_is_expenses_included_in_cost_budget,
            cast(null as numeric) as project_fee_amount,
            cast(null as numeric) as project_budget_amount,
            cast(null as numeric) as project_over_budget_notification_pct,
            cast(null as {{ dbt_utils.type_string() }}) as project_budget_by
        from source p
    )
select *
from renamed
