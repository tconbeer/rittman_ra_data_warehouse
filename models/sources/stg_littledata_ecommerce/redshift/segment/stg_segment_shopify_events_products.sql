{{ config(enabled=target.type == "redshift") }}
{% if var("crm_warehouse_contact_sources") %}
    {% if "segment_shopify_events" in var("crm_warehouse_contact_sources") %}

        with
            products as (
                select products
                from
                    {{
                        var(
                            "stg_segment_shopify_events_segment_checkout_started_table"
                        )
                    }}
                where is_valid_json_array(products)
                union all
                select products
                from
                    {{
                        var(
                            "stg_segment_shopify_events_segment_order_completed_table"
                        )
                    }}
                where is_valid_json_array(products)

            ),
            products_deduped as (
                select
                    json_extract_path_text(
                        replace(replace(products, '[', ''), ']', ''), 'sku'
                    ) as sku,
                    json_extract_path_text(
                        replace(replace(products, '[', ''), ']', ''), 'product_id'
                    ) as product_id,
                    json_extract_path_text(
                        replace(replace(products, '[', ''), ']', ''),
                        'shopify_product_id'
                    ) as shopify_product_id,
                    json_extract_path_text(
                        replace(replace(products, '[', ''), ']', ''),
                        'shopify_variant_id'
                    ) as shopify_variant_id,
                    json_extract_path_text(
                        replace(replace(products, '[', ''), ']', ''), 'brand'
                    ) as brand,
                    json_extract_path_text(
                        replace(replace(products, '[', ''), ']', ''), 'category'
                    ) as category,
                    json_extract_path_text(
                        replace(replace(products, '[', ''), ']', ''), 'name'
                    ) as name,
                    json_extract_path_text(
                        replace(replace(products, '[', ''), ']', ''), 'price'
                    ) as price,
                    json_extract_path_text(
                        replace(replace(products, '[', ''), ']', ''), 'variant'
                    ) as variant,
                    case
                        when regexp_count(products, 'Sample') > 0 then true else false
                    end as product_is_sample
                from products
                group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
            )
        select
            {{
                dbt_utils.hash(
                    "concat( 														concat( 															concat( 																cast(shopify_product_id as dbt_utils.type_string() ), 																cast(shopify_variant_id as dbt_utils.type_string() ) 															), 															cast(category as dbt_utils.type_string() ) 														), 														cast(variant as  dbt_utils.type_string() ) 													)"
                )
            }}
            as product_uid,
            *
        from products_deduped
        order by 3, 4, 9

    {% else %} {{ config(enabled=false) }}
    {% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
