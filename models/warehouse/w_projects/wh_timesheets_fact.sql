{% if var("projects_warehouse_timesheet_sources") %}
{{ config(unique_key="timesheet_projects_pk", alias="timesheets_fact") }}

with
    {% if target.type == "bigquery" %}
    companies_dim as (

        select {{ dbt_utils.star(from=ref("wh_companies_dim")) }}
        from {{ ref("wh_companies_dim") }}
    ),
    contacts_dim as (
        select {{ dbt_utils.star(from=ref("wh_contacts_dim")) }}
        from {{ ref("wh_contacts_dim") }}
    ),
    {% elif target.type == "snowflake" %}
    companies_dim as (
        select c.company_pk, cf.value::string as company_id
        from {{ ref("wh_companies_dim") }} c, table(flatten(c.all_company_ids)) cf
    ),
    contacts_dim as (
        select c.contact_pk, cf.value::string as contact_id
        from {{ ref("wh_contacts_dim") }} c, table(flatten(c.all_contact_ids)) cf
    ),

    {% else %}
    {{
        exceptions.raise_compiler_error(
            target.type ~ " not supported in this project"
        )
    }}
    {% endif %}
    tasks_dim as (
        select {{ dbt_utils.star(from=ref("wh_timesheet_tasks_dim")) }}
        from {{ ref("wh_timesheet_tasks_dim") }}
    ),
    projects_dim as (
        select {{ dbt_utils.star(from=ref("wh_timesheet_projects_dim")) }}
        from {{ ref("wh_timesheet_projects_dim") }}
    ),
    timesheets as (
        select {{ dbt_utils.star(from=ref("int_timesheets")) }}
        from {{ ref("int_timesheets") }}
    )
select
    {{ dbt_utils.surrogate_key(["timesheet_id"]) }} as timesheet_pk,
    c.company_pk,
    u.contact_pk,
    p.timesheet_project_pk,
    ta.timesheet_task_pk,
    timesheet_invoice_id,
    timesheet_billing_date,
    min(timesheet_billing_date) over (
        partition by c.company_pk
        order by
            timesheet_billing_date range
            between unbounded preceding and unbounded following
    ) as first_company_timesheet_billing_date,
    max(timesheet_billing_date) over (
        partition by c.company_pk
        order by
            timesheet_billing_date range
            between unbounded preceding and unbounded following
    ) as last_company_timesheet_billing_date,
    timesheet_hours_billed,
    timesheet_total_amount_billed,
    timesheet_is_billable,
    timesheet_has_been_billed,
    timesheet_has_been_locked,
    timesheet_billable_hourly_rate_amount,
    timesheet_billable_hourly_cost_amount,
    timesheet_notes
from timesheets t

{% if target.type == "bigquery" %}
join companies_dim c on t.company_id in unnest(c.all_company_ids)
join contacts_dim u on cast(t.timesheet_users_id as string) in unnest(u.all_contact_ids)
{% elif target.type == "snowflake" %}
join companies_dim c on t.company_id = c.company_id
join contacts_dim u on t.timesheet_users_id::string = u.contact_id
    {% else %}
    {{
        exceptions.raise_compiler_error(
            target.type ~ " not supported in this project"
        )
    }}
    {% endif %}
left outer join projects_dim p on t.timesheet_project_id = p.timesheet_project_id
left outer join tasks_dim ta on t.timesheet_task_id = ta.task_id
{% else %} {{ config(enabled=false) }}
{% endif %}
