with
    numbers as (
        select
            blended_user_id,
            sum(count_conversions) as count_conversions,
            sum(count_order_conversions) as count_order_conversions,
            sum(count_first_order_conversions) count_first_order_conversions,
            sum(count_registration_conversions) count_registration_conversions,
            sum(count_repeat_order_conversions) count_repeat_order_conversions,
            sum(first_click_attrib_pct) first_click_attrib_pct,
            sum(first_non_direct_click_attrib_pct) first_non_direct_click_attrib_pct,
            sum(first_paid_click_attrib_pct) first_paid_click_attrib_pct,
            sum(last_click_attrib_pct) last_click_attrib_pct,
            sum(last_non_direct_click_attrib_pct) last_non_direct_click_attrib_pct,
            sum(last_paid_click_attrib_pct) last_paid_click_attrib_pct,
            sum(even_click_attrib_pct) even_click_attrib_pct,
            sum(time_decay_attrib_pct) time_decay_attrib_pct,
            sum(
                user_registration_first_click_attrib_conversions
            ) user_registration_first_click_attrib_conversions,
            sum(
                user_registration_first_non_direct_click_attrib_conversions
            ) user_registration_first_non_direct_click_attrib_conversions,
            sum(
                user_registration_first_paid_click_attrib_conversions
            ) user_registration_first_paid_click_attrib_conversions,
            sum(
                user_registration_last_click_attrib_conversions
            ) user_registration_last_click_attrib_conversions,
            sum(
                user_registration_last_non_direct_click_attrib_conversions
            ) user_registration_last_non_direct_click_attrib_conversions,
            sum(
                user_registration_last_paid_click_attrib_conversions
            ) user_registration_last_paid_click_attrib_conversions,
            sum(
                user_registration_even_click_attrib_conversions
            ) user_registration_even_click_attrib_conversions,
            sum(
                user_registration_time_decay_attrib_conversions
            ) user_registration_time_decay_attrib_conversions,
            sum(
                first_order_first_click_attrib_conversions
            ) first_order_first_click_attrib_conversions,
            sum(
                first_order_first_non_direct_click_attrib_conversions
            ) first_order_first_non_direct_click_attrib_conversions,
            sum(
                first_order_last_click_attrib_conversions
            ) first_order_last_click_attrib_conversions,
            sum(
                first_order_last_non_direct_click_attrib_conversions
            ) first_order_last_non_direct_click_attrib_conversions,
            sum(
                first_order_last_paid_click_attrib_conversions
            ) first_order_last_paid_click_attrib_conversions,
            sum(
                first_order_even_click_attrib_conversions
            ) first_order_even_click_attrib_conversions,
            sum(
                first_order_time_decay_attrib_conversions
            ) first_order_time_decay_attrib_conversions,
            sum(
                repeat_order_first_click_attrib_conversions
            ) repeat_order_first_click_attrib_conversions,
            sum(
                repeat_order_first_paid_click_attrib_conversions
            ) repeat_order_first_paid_click_attrib_conversions,
            sum(
                repeat_order_last_click_attrib_conversions
            ) repeat_order_last_click_attrib_conversions,
            sum(
                repeat_order_last_non_direct_click_attrib_conversions
            ) repeat_order_last_non_direct_click_attrib_conversions,
            sum(
                repeat_order_last_paid_click_attrib_conversions
            ) repeat_order_last_paid_click_attrib_conversions,
            sum(
                repeat_order_even_click_attrib_conversions
            ) repeat_order_even_click_attrib_conversions,
            sum(
                repeat_order_time_decay_attrib_conversions
            ) repeat_order_time_decay_attrib_conversions,
            sum(first_order_total_revenue) first_order_total_revenue,
            sum(
                first_order_first_click_attrib_revenue
            ) first_order_first_click_attrib_revenue,
            sum(
                first_order_first_non_direct_click_attrib_revenue
            ) first_order_first_non_direct_click_attrib_revenue,
            sum(
                first_order_first_paid_click_attrib_revenue
            ) first_order_first_paid_click_attrib_revenue,
            sum(
                first_order_last_click_attrib_revenue
            ) first_order_last_click_attrib_revenue,
            sum(
                first_order_last_non_direct_click_attrib_revenue
            ) first_order_last_non_direct_click_attrib_revenue,
            sum(
                first_order_last_paid_click_attrib_revenue
            ) first_order_last_paid_click_attrib_revenue,
            sum(
                first_order_even_click_attrib_revenue
            ) first_order_even_click_attrib_revenue,
            sum(
                first_order_time_decay_attrib_revenue
            ) first_order_time_decay_attrib_revenue,
            sum(repeat_order_total_revenue) repeat_order_total_revenue,
            sum(
                repeat_order_first_click_attrib_revenue
            ) repeat_order_first_click_attrib_revenue,
            sum(
                repeat_order_first_non_direct_click_attrib_revenue
            ) repeat_order_first_non_direct_click_attrib_revenue,
            sum(
                repeat_order_first_paid_click_attrib_revenue
            ) repeat_order_first_paid_click_attrib_revenue,
            sum(
                repeat_order_last_click_attrib_revenue
            ) repeat_order_last_click_attrib_revenue,
            sum(
                repeat_order_last_non_direct_click_attrib_revenue
            ) repeat_order_last_non_direct_click_attrib_revenue,
            sum(
                repeat_order_last_paid_click_attrib_revenue
            ) repeat_order_last_paid_click_attrib_revenue,
            sum(
                repeat_order_even_click_attrib_revenue
            ) repeat_order_even_click_attrib_revenue,
            sum(
                repeat_order_time_decay_attrib_revenue
            ) repeat_order_time_decay_attrib_revenue
        from {{ ref("attribution_fact") }}
        group by 1
    ),
    totals as (
        select
            sum(count_conversions) as count_conversions,
            sum(count_first_order_conversions) as count_first_order_conversions,
            sum(count_repeat_order_conversions) as count_repeat_order_conversions,
            sum(first_order_total_revenue) as first_order_total_revenue,
            sum(
                repeat_order_first_click_attrib_revenue
            ) as repeat_order_first_click_attrib_revenue,
            sum(
                repeat_order_last_click_attrib_revenue
            ) as repeat_order_last_click_attrib_revenue,
            sum(
                repeat_order_even_click_attrib_revenue
            ) as repeat_order_even_click_attrib_revenue,
            sum(
                repeat_order_time_decay_attrib_revenue
            ) as repeat_order_time_decay_attrib_revenue,
            sum(
                first_order_first_click_attrib_revenue
            ) as first_order_first_click_attrib_revenue,
            sum(
                first_order_last_click_attrib_revenue
            ) as first_order_last_click_attrib_revenue,
            sum(
                first_order_even_click_attrib_revenue
            ) as first_order_even_click_attrib_revenue,
            sum(
                first_order_time_decay_attrib_revenue
            ) as first_order_time_decay_attrib_revenue,
            sum(repeat_order_total_revenue) as repeat_order_total_revenue,
            sum(count_registration_conversions) as count_registration_conversions,
            sum(
                user_registration_first_click_attrib_conversions
            ) as user_registration_first_click_attrib_conversions,
            sum(
                user_registration_last_click_attrib_conversions
            ) as user_registration_last_click_attrib_conversions,
            sum(
                user_registration_even_click_attrib_conversions
            ) as user_registration_even_click_attrib_conversions,
            sum(
                user_registration_time_decay_attrib_conversions
            ) as user_registration_time_decay_attrib_conversions,
            sum(
                first_order_first_click_attrib_conversions
            ) as first_order_first_click_attrib_conversions,
            sum(
                first_order_last_click_attrib_conversions
            ) as first_order_last_click_attrib_conversions,
            sum(
                first_order_even_click_attrib_conversions
            ) as first_order_even_click_attrib_conversions,
            sum(
                first_order_time_decay_attrib_conversions
            ) as first_order_time_decay_attrib_conversions,
            sum(
                repeat_order_first_click_attrib_conversions
            ) as repeat_order_first_click_attrib_conversions,
            sum(
                repeat_order_last_click_attrib_conversions
            ) as repeat_order_last_click_attrib_conversions,
            sum(
                repeat_order_even_click_attrib_conversions
            ) as repeat_order_even_click_attrib_conversions,
            sum(
                repeat_order_time_decay_attrib_conversions
            ) as repeat_order_time_decay_attrib_conversions
        from numbers
    )
select *
from
    (
        select
            'conversion counts' as test,
            (
                count_conversions
                - count_registration_conversions
                - count_first_order_conversions
                - count_repeat_order_conversions
            )
            = 0 as pass
        from totals
        union all
        select
            'first_order_revenue' as test,
            case
                when
                    (
                        first_order_total_revenue = round(
                            first_order_first_click_attrib_revenue
                        )
                    )
                    and (
                        first_order_total_revenue = round(
                            first_order_last_click_attrib_revenue
                        )
                    )
                    and (
                        first_order_total_revenue = round(
                            first_order_even_click_attrib_revenue
                        )
                    )
                    and (
                        first_order_total_revenue = round(
                            first_order_time_decay_attrib_revenue
                        )
                    )
                then true
                else false
            end as pass
        from totals
        union all
        select
            'repeat_order_revenue' as test,
            case
                when
                    (
                        repeat_order_total_revenue = round(
                            repeat_order_first_click_attrib_revenue
                        )
                    )
                    and (
                        repeat_order_total_revenue = round(
                            repeat_order_last_click_attrib_revenue
                        )
                    )
                    and (
                        repeat_order_total_revenue = round(
                            repeat_order_even_click_attrib_revenue
                        )
                    )
                    and (
                        repeat_order_total_revenue = round(
                            repeat_order_time_decay_attrib_revenue
                        )
                    )
                then true
                else false
            end as pass
        from totals
    )
where pass = false
