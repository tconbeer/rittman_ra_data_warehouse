{% if not var("enable_custom_source_2") %} {{ config(enabled=false) }} {% endif %}

with
    source as (select * from {{ source("custom_source_2", "s_transactions") }}),
    renamed as (
        select
            concat('custom_2-', id) as transaction_id,
            cast(null as {{ dbt_utils.type_string() }}) as transaction_description,
            cast(null as {{ dbt_utils.type_string() }}) as transaction_currency,
            cast(null as numeric) as transaction_exchange_rate,
            cast(null as numeric) as transaction_gross_amount,
            cast(null as numeric) as transaction_fee_amount,
            cast(null as numeric) as transaction_tax_amount,
            cast(null as numeric) as transaction_net_amount,
            cast(null as {{ dbt_utils.type_string() }}) as transaction_status,
            cast(null as {{ dbt_utils.type_string() }}) as transaction_type,
            cast(null as {{ dbt_utils.type_timestamp() }}) as transaction_created_ts,
            cast(null as {{ dbt_utils.type_timestamp() }}) as transaction_updated_ts
        from source
    )
select *
from renamed
