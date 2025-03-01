{% if var("finance_warehouse_transaction_sources") %}

    {{
        config(
            alias="transactions_fact",
            unique_key="transaction_id",
            incremental_strategy="merge",
            materialized="incremental",
        )
    }}

    with transactions as (select * from {{ ref("int_transactions") }})

    select {{ dbt_utils.surrogate_key(["transaction_id"]) }} as transaction_pk, *
    from transactions

    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where
            transaction_created_ts
            > (select max(transaction_created_ts) from {{ this }})
            or transaction_last_modified_ts
            > (select max(transaction_last_modified_ts) from {{ this }})
    {% endif %}

{% else %} {{ config(enabled=false) }}
{% endif %}
