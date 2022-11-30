{{ config(enabled=target.type == "bigquery") }}
{% if var("product_warehouse_usage_sources") %}
{% if "bigquery_usage" in var("product_warehouse_usage_sources") %}


with
    source as (
        select *
        from {{ source("bigquery_usage_product_usage", "cloudaudit_data_access") }}
    ),
    renamed as (
        select
            protopayload_auditlog.authenticationinfo.principalemail
            as product_account_id,
            coalesce(
                concat(
                    '{{ var(' stg_bigquery_usage_id - prefix ') }}',
                    protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.querypriority
                ),
                'N/A'
            ) as product_id,
            coalesce(
                concat(
                    '{{ var(' stg_bigquery_usage_id - prefix ') }}',
                    protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.statementtype
                ),
                'N/A'
            ) as product_sku_id,
            concat(
                '{{ var(' stg_bigquery_usage_id - prefix ') }}',
                resource.labels.project_id
            ) as company_id,
            resource.labels.project_id as product_project_id,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.createtime
            as product_usage_billing_ts,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.starttime
            as product_usage_start_ts,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.endtime
            as product_usage_end_ts,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobname.location
            as product_usage_location,
            cast(null as {{ dbt_utils.type_string() }}) as product_usage_country,
            cast(null as {{ dbt_utils.type_string() }}) as product_usage_region,
            cast(null as {{ dbt_utils.type_string() }}) as product_usage_zone,
            "bytes" as product_usage_unit,
            'GBP' as product_usage_currency,
            (
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.totalbilledbytes
                / 1099511627776
            )
            * .72 as product_usage_cost,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.totalprocessedbytes
            as product_usage_amount,
            0.72 as product_currency_conversion_rate,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.queryoutputrowcount
            as product_usage_row_count,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.query
            as product_usage_query_text,
            md5(
                lower(
                    replace(
                        protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.query,
                        ' ',
                        ''
                    )
                )
            ) as product_usage_query_hash,
            cast(
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.querypriority
                as string
            ) as product_usage_priority,
            cast(
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatus.state
                as string
            ) as product_usage_status,
            cast(
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatus.error.code
                as string
            ) as product_usage_error_code,
            cast(
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatus.error.message
                as string
            ) as product_usage_error_status,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobname.jobid
            as product_usage_job_id,
            cast(null as {{ dbt_utils.type_string() }}) as contact_id

        from source
        where
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.statementtype
            is not null
    )
select *
from renamed

{% else %} {{ config(enabled=false) }}
{% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
