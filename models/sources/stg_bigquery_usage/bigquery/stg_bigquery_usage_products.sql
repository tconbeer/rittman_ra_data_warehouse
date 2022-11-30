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
            coalesce(
                concat(
                    '{{ var(' stg_bigquery_usage_id - prefix ') }}',
                    protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.statementtype
                ),
                'N/A'
            ) as product_sku_id,
            coalesce(
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.statementtype,
                'N/A'
            ) as product_sku_name,
            coalesce(
                concat(
                    '{{ var(' stg_bigquery_usage_id - prefix ') }}',
                    protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.querypriority
                ),
                'N/A'
            ) as product_id,
            coalesce(
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.querypriority,
                'N/A'
            ) as product_name,
            concat(
                '{{ var(' stg_bigquery_usage_id - prefix ') }}', 'BigQuery Usage Export'
            ) as product_source_id,
            'BigQuery Usage Export' as product_source_name
        from source
        group by 1, 2, 3, 4, 5, 6
    )
select *
from renamed

{% else %} {{ config(enabled=false) }}
{% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
