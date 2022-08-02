{{ config(enabled=target.type == "bigquery") }}
{% if not var("enable_gcp_billing_source") %} {{ config(enabled=false) }} {% endif %}

with
    source as (select * from {{ source("gcp_billing", "gcp_billing_export") }}),
    renamed as (
        select
            billing_account_id,
            project.id as project_id,
            location.location as billing_data_location,
            location.country as billing_data_country,
            location.region,
            location.zone,
            sum(cost) as total_cost,
            sum(coalesce(usage.amount, 0)) as total_usage_amount,
            usage.unit,
            sum(
                coalesce(usage.amount_in_pricing_units, 0)
            ) as total_amount_in_pricing_units,
            usage.pricing_unit,
            currency,
            avg(coalesce(currency_conversion_rate, 0)) as avg_currency_conversion_rate,
            invoice.month as billing_month,
            service.id as service_id,
            service.description
        from source
        group by 1, 2, 3, 4, 5, 6, 9, 11, 12, 14, 15, 16
    )
select *
from renamed
