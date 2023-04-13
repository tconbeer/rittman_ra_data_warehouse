{{ config(enabled=target.type == "redshift") }}
{% if var("ecommerce_warehouse_order_sources") %}
    {% if "segment_shopify_events" in var("ecommerce_warehouse_order_sources") %}

        with
            orders as (
                select *
                from
                    {{
                        var(
                            "stg_segment_shopify_events_segment_order_completed_table"
                        )
                    }}
                where is_valid_json_array(products)
            ),
            refunded as (
                select
                    order_id,
                    timestamp as order_refunded_ts,
                    presentment_amount as presentment_refunded_amount
                from
                    {{ var("stg_segment_shopify_events_segment_order_refunded_table") }}
            ),
            deleted as (
                select order_id, timestamp as order_deleted_ts
                from {{ var("stg_segment_shopify_events_segment_order_deleted_table") }}
            )
        select
            orders.order_id,
            checkout_id as order_checkout_id,
            event,
            event_text,
            original_timestamp as order_ts,
            md5(
                concat(
                    concat(
                        concat(
                            json_extract_path_text(
                                replace(replace(products, '[', ''), ']', ''),
                                'shopify_product_id'
                            )::varchar,
                            json_extract_path_text(
                                replace(replace(products, '[', ''), ']', ''),
                                'shopify_variant_id'
                            )::varchar
                        ),
                        json_extract_path_text(
                            replace(replace(products, '[', ''), ']', ''), 'category'
                        )::varchar
                    ),
                    json_extract_path_text(
                        replace(replace(products, '[', ''), ']', ''), 'variant'
                    )::varchar
                )
            ) as product_uid,
            user_id as user_id,
            presentment_amount,
            shipping,
            tax,
            currency,
            subtotal,
            total as order_total,
            coupon,
            presentment_currency,
            discount,
            affiliation,
            order_refunded_ts,
            presentment_refunded_amount,
            case
                when order_refunded_ts is null and order_deleted_ts is null
                then true
                else false
            end as is_paid_order
        from orders
        left join refunded on orders.order_id = refunded.order_id
        left join deleted on orders.order_id = deleted.order_id
        order by 1
    {% else %} {{ config(enabled=false) }}
    {% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
