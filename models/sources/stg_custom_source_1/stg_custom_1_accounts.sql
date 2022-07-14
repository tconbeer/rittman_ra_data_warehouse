{% if not var("enable_custom_source_1") %} {{ config(enabled=false) }} {% endif %}

with
    source as (
        select country_name, sum(c) c
        from
            (
                select ip, country_name, c
                from
                    (
                        select
                            *,
                            net.safe_ip_from_string(ip) & net.ip_net_mask(
                                4, mask
                            ) network_bin
                        from source_of_ip_addresses, unnest(generate_array(9, 32)) mask
                        where byte_length(net.safe_ip_from_string(ip)) = 4
                    )
                join `` using(network_bin, mask)
            )
        group by 1
        order by 2 desc {{ source("custom_source_1", "s_accounts") }}
    ),
    renamed as
    (
        select
            concat('custom_1-', id) account_id,
            cast(null as {{ dbt_utils.type_string() }}) as account_name,
            cast(null as {{ dbt_utils.type_string() }}) as account_code,
            cast(null as {{ dbt_utils.type_string() }}) as account_type,
            cast(null as {{ dbt_utils.type_string() }}) as account_class,
            cast(null as {{ dbt_utils.type_string() }}) as account_status,
            cast(null as {{ dbt_utils.type_string() }}) as account_description,
            cast(null as {{ dbt_utils.type_string() }}) as account_reporting_code,
            cast(null as {{ dbt_utils.type_string() }}) as account_reporting_code_name,
            cast(null as {{ dbt_utils.type_string() }}) as account_currency_code,
            cast(null as {{ dbt_utils.type_string() }}) as account_bank_account_type,
            cast(null as {{ dbt_utils.type_string() }}) as account_bank_account_number,
            cast(null as {{ dbt_utils.type_string() }}) as account_is_system_account,
            cast(null as {{ dbt_utils.type_string() }}) as account_tax_type,
            cast(
                null as {{ dbt_utils.type_string() }}
            ) as account_show_in_expense_claims,
            cast(
                null as {{ dbt_utils.type_string() }}
            ) as account_enable_payments_to_account
        from source
    )
select *
from renamed
