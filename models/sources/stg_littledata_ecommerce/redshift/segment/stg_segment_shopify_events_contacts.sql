{{ config(enabled=target.type == "redshift") }}
{% if var("crm_warehouse_contact_sources") %}
    {% if "segment_shopify_events" in var("crm_warehouse_contact_sources") %}

        with
            source as (

                select u.*, n.sample_orders, n.non_sample_orders
                from {{ var("stg_segment_shopify_events_segment_users_table") }} u
                left join
                    (
                        select
                            json_extract_path_text(
                                replace(
                                    replace(context_external_ids, '[', ''), ']', ''
                                ),
                                'id'
                            ) as user_id,
                            sum(
                                case
                                    when regexp_count(products, 'Sample') < 1
                                    then 1
                                    else 0
                                end
                            ) as non_sample_orders,
                            sum(
                                case
                                    when regexp_count(products, 'Sample') > 0
                                    then 1
                                    else 0
                                end
                            ) as sample_orders
                        from production_shopify_by_littledata.order_completed
                        group by 1
                    ) n
                    on u.id = n.user_id

            ),
            renamed as (
                select

                    cast(id as {{ dbt_utils.type_string() }}) as contact_id,
                    first_name as first_name,
                    last_name as last_name,
                    concat(concat(first_name, ' '), last_name) as contact_name,
                    cast(null as {{ dbt_utils.type_string() }}) contact_job_title,
                    email as contact_email,
                    phone as contact_phone,
                    address_street contact_address,
                    address_city contact_city,
                    address_state contact_state,
                    address_country as contact_country,
                    address_postal_code contact_postcode_zip,
                    cast(null as {{ dbt_utils.type_string() }}) contact_company,
                    cast(null as {{ dbt_utils.type_string() }}) contact_website,
                    cast(null as {{ dbt_utils.type_string() }}) as contact_company_id,
                    cast(null as {{ dbt_utils.type_string() }}) as contact_owner_id,
                    cast(
                        null as {{ dbt_utils.type_string() }}
                    ) as contact_lifecycle_stage,
                    cast(null as {{ dbt_utils.type_boolean() }}) as contact_is_staff,
                    state = 'enabled' as contact_is_active,
                    coalesce(
                        (
                            regexp_count(tags, 'PRO,') > 0
                            or regexp_count(tags, 'PRO_25,') > 0
                            or regexp_count(tags, 'PRO_20,') > 0
                            or regexp_count(tags, 'PRO_15,') > 0
                            or regexp_count(tags, 'PRO_FACEBOOK')
                        ),
                        false
                    ) as contact_is_pro,
                    marketing_opt_in as contact_is_marketing_opt_in,
                    customer_lifetime_value as contact_lifetime_value,
                    purchase_count as contact_purchase_count,
                    verified_email as contact_has_verified_email,
                    accepts_marketing as contact_accepts_marketing,
                    non_sample_orders as contact_non_sample_orders,
                    sample_orders as contact_sample_orders,
                    created_at as contact_created_date,
                    cast(
                        null as {{ dbt_utils.type_timestamp() }}
                    ) as contact_last_modified_date
                from source
            )
        select *
        from renamed

    {% else %} {{ config(enabled=false) }}
    {% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
