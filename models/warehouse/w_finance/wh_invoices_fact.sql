{% if var("finance_warehouse_invoice_sources") %}

    {{ config(unique_key="invoice_pk", alias="invoices_fact") }}

    with
        invoices as (select * from {{ ref("int_invoices") }}),
        companies_dim as (select * from {{ ref("wh_companies_dim") }})
        {% if "harvest_projects" in var("projects_warehouse_timesheet_sources") %}
            , projects_dim as (select * from {{ ref("wh_timesheet_projects_dim") }})
        {% endif %}
    select
        {{ dbt_utils.surrogate_key(["invoice_number"]) }} as invoice_pk,
        c.company_pk,
        row_number() over (
            partition by c.company_pk order by invoice_sent_at_ts
        ) as invoice_seq,
        {{
            dbt_utils.datediff(
                "min(date(invoice_sent_at_ts)) over (partition by c.company_pk RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
                "invoice_sent_at_ts",
                "MONTH",
            )
        }}
        as months_since_first_invoice,

        {{
            dbt_utils.date_trunc(
                "MONTH",
                "MIN(date(invoice_sent_at_ts))                               OVER (partition by c.company_pk                               RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
            )
        }} first_invoice_month,
        {{
            dbt_utils.date_trunc(
                "MONTH",
                "MAX(date(invoice_sent_at_ts))                               OVER (partition by c.company_pk                               RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
            )
        }} last_invoice_month,
        {{
            dbt_utils.datediff(
                "invoice_sent_at_ts",
                "max(date(invoice_sent_at_ts)) over (partition by c.company_pk RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
                "MONTH",
            )
        }}
        as months_before_last_invoice,
        {{ dbt_utils.datediff("invoice_sent_at_ts", "current_timestamp", "MONTH") }}
        as invoice_months_before_now,

        {{
            dbt_utils.datediff(
                "min(date(invoice_sent_at_ts)) over (partition by c.company_pk  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
                "date(invoice_sent_at_ts)",
                "QUARTER",
            )
        }}
        as quarters_since_first_invoice,
        {{
            dbt_utils.date_trunc(
                "QUARTER",
                "min(date(invoice_sent_at_ts)) over (partition by c.company_pk RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
            )
        }} first_invoice_quarter,
        {% if "harvest_projects" in var("projects_warehouse_timesheet_sources") %}
            /* s.user_pk as creator_users_pk, */
            p.timesheet_project_pk,
        {% endif %}
        i.*
    from invoices i
    join companies_dim c on i.company_id in unnest(c.all_company_ids)
    {% if "harvest_projects" in var("projects_warehouse_timesheet_sources") %}
        /*JOIN user_dim s
   ON cast(i.invoice_creator_users_id as string) IN UNNEST(s.all_user_ids)*/
        left outer join
            projects_dim p on cast(i.project_id as string) = p.timesheet_project_id
    {% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
