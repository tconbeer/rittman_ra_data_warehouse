{% if var("marketing_warehouse_ad_campaign_sources") and var(
    "product_warehouse_event_sources"
) %}
{{ config(alias="attribution_fact") }}


with
    events_filtered as (
        select *
        from
            (
                select
                    *,
                    first_value(
                        case
                            when
                                event_type
                                = '{{ var(' attribution_create_account_event_type ') }}'
                            then event_id
                        end
                    ) over (
                        partition by blended_user_id
                        order by event_ts
                        rows between unbounded preceding and unbounded following
                    ) as first_registration_event_id,
                    first_value(
                        case
                            when
                                event_type
                                = '{{ var(' attribution_conversion_event_type ') }}'
                            then event_id
                        end
                    ) over (
                        partition by blended_user_id
                        order by event_ts
                        rows between unbounded preceding and unbounded following
                    ) as first_order_event_id
                from {{ ref("wh_web_events_fact") }}
            )
        where
            event_type != '{{ var(' attribution_create_account_event_type ') }}'
            or (event_id = first_registration_event_id)
    ),
    converting_events as (
        select
            e.blended_user_id,
            session_id,
            event_type,
            order_id,
            case
                when
                    event_type = '{{ var(' attribution_conversion_event_type ') }}'
                    and event_id = first_order_event_id
                then total_revenue
                else 0
            end as first_order_total_revenue,
            case
                when
                    event_type = '{{ var(' attribution_conversion_event_type ') }}'
                    and event_id != first_order_event_id
                then total_revenue
                else 0
            end as repeat_order_total_revenue,
            currency_code,
            case
                when
                    event_type in (
                        '{{ var(' attribution_conversion_event_type ') }}',
                        '{{ var(' attribution_create_account_event_type ') }}'
                    )
                then 1
                else 0
            end as count_conversions,
            case
                when
                    event_type = '{{ var(' attribution_conversion_event_type ') }}'
                    and event_id = first_order_event_id
                then 1
                else 0
            end as count_first_order_conversions,
            case
                when
                    event_type = '{{ var(' attribution_conversion_event_type ') }}'
                    and event_id != first_order_event_id
                then 1
                else 0
            end as count_repeat_order_conversions,
            case
                when event_type = '{{ var(' attribution_conversion_event_type ') }}'
                then 1
                else 0
            end as count_order_conversions,
            case
                when event_type = '{{ var(' attribution_create_account_event_type ') }}'
                then 1
                else 0
            end as count_registration_conversions,
            event_ts as converted_ts
        from events_filtered e
        where
            event_type in (
                '{{ var(' attribution_conversion_event_type ') }}',
                '{{ var(' attribution_create_account_event_type ')}}'
            )
    ),
    converting_sessions_deduped as (
        select
            session_id session_id,
            max(blended_user_id) as blended_user_id,
            sum(first_order_total_revenue) as first_order_total_revenue,
            sum(repeat_order_total_revenue) as repeat_order_total_revenue,
            max(currency_code) as currency_code,
            sum(count_first_order_conversions) as count_first_order_conversions,
            sum(count_repeat_order_conversions) as count_repeat_order_conversions,
            sum(count_order_conversions) as count_order_conversions,
            sum(count_registration_conversions) as count_registration_conversions,
            sum(count_registration_conversions)
            + sum(count_first_order_conversions)
            + sum(count_repeat_order_conversions) as count_conversions,
            max(converted_ts) as converted_ts
        from converting_events
        group by 1
    ),
    converting_sessions_deduped_labelled as (
        select *
        from
            (
                select
                    *,
                    first_value(converted_ts ignore nulls) over (
                        partition by blended_user_id
                        order by session_start_ts
                        rows between current row and unbounded following
                    ) as conversion_cycle_conversion_ts
                from
                    (
                        select
                            s.blended_user_id,
                            s.session_start_ts,
                            s.session_end_ts,
                            (
                                select c.converted_ts
                                from converting_sessions_deduped c
                                where c.session_id = s.session_id
                            ) as converted_ts,
                            s.session_id as session_id,
                            row_number() over (
                                partition by s.blended_user_id
                                order by s.session_start_ts
                            ) as session_seq,
                            (
                                select max(c.count_conversions)
                                from converting_sessions_deduped c
                                where c.session_id = s.session_id
                            ) as count_conversions,
                            (
                                select max(c.count_order_conversions)
                                from converting_sessions_deduped c
                                where c.session_id = s.session_id
                            ) as count_order_conversions,
                            (
                                select max(c.count_first_order_conversions)
                                from converting_sessions_deduped c
                                where c.session_id = s.session_id
                            ) as count_first_order_conversions,
                            (
                                select max(c.count_repeat_order_conversions)
                                from converting_sessions_deduped c
                                where c.session_id = s.session_id
                            ) as count_repeat_order_conversions,
                            (
                                select max(c.count_registration_conversions)
                                from converting_sessions_deduped c
                                where c.session_id = s.session_id
                            ) as count_registration_conversions,
                            coalesce(
                                (
                                    select
                                        case
                                            when (c.session_id = s.session_id)
                                            then true
                                            else false
                                        end
                                    from converting_sessions_deduped c
                                    where c.session_id = s.session_id
                                ),
                                false
                            ) as conversion_session,
                            coalesce(
                                (
                                    select
                                        case
                                            when (c.session_id = s.session_id)
                                            then 1
                                            else 0
                                        end
                                    from converting_sessions_deduped c
                                    where c.session_id = s.session_id
                                ),
                                0
                            ) as conversion_event,
                            coalesce(
                                (
                                    select
                                        case
                                            when
                                                (
                                                    c.session_id = s.session_id
                                                    and c.count_order_conversions > 1
                                                )
                                            then 1
                                            else 0
                                        end
                                    from converting_sessions_deduped c
                                    where c.session_id = s.session_id
                                ),
                                0
                            ) as order_conversion_event,
                            coalesce(
                                (
                                    select
                                        case
                                            when
                                                (
                                                    c.session_id = s.session_id
                                                    and c.count_registration_conversions
                                                    > 1
                                                )
                                            then 1
                                            else 0
                                        end
                                    from converting_sessions_deduped c
                                    where c.session_id = s.session_id
                                ),
                                0
                            ) as registration_conversion_event,
                            coalesce(
                                (
                                    select
                                        case
                                            when
                                                (
                                                    c.session_id = s.session_id
                                                    and c.count_first_order_conversions
                                                    > 1
                                                )
                                            then 1
                                            else 0
                                        end
                                    from converting_sessions_deduped c
                                    where c.session_id = s.session_id
                                ),
                                0
                            ) as first_order_conversion_event,
                            coalesce(
                                (
                                    select
                                        case
                                            when
                                                (
                                                    c.session_id = s.session_id
                                                    and c.count_repeat_order_conversions
                                                    > 1
                                                )
                                            then 1
                                            else 0
                                        end
                                    from converting_sessions_deduped c
                                    where c.session_id = s.session_id
                                ),
                                0
                            ) as repeat_order_conversion_event,
                            utm_source,
                            utm_content,
                            utm_medium,
                            utm_campaign,
                            referrer_host,
                            first_page_url_host,
                            split(net.reg_domain(referrer_host), '.')[
                                offset(0)
                            ] as referrer_domain,
                            channel,
                            case
                                when lower(channel) = 'direct' then false else true
                            end as is_non_direct_channel,
                            case
                                when lower(channel) like '%paid%' then true else false
                            end as is_paid_channel,
                            events,
                            (
                                select c.first_order_total_revenue
                                from converting_sessions_deduped c
                                where c.session_id = s.session_id
                            ) as first_order_total_revenue,
                            (
                                select c.repeat_order_total_revenue
                                from converting_sessions_deduped c
                                where c.session_id = s.session_id
                            ) as repeat_order_total_revenue,
                            (
                                select c.currency_code
                                from converting_sessions_deduped c
                                where c.session_id = s.session_id
                            ) as currency_code
                        from {{ ref("wh_web_sessions_fact") }} s
                    )
            )
        where conversion_cycle_conversion_ts >= session_start_ts
    ),
    converting_sessions_deduped_labelled_with_conversion_number as (
        select
            *,
            sum(conversion_event) over (
                partition by blended_user_id
                order by session_start_ts
                rows between unbounded preceding and current row
            ) as user_total_conversions,
            sum(count_order_conversions) over (
                partition by blended_user_id
                order by session_start_ts
                rows between unbounded preceding and current row
            ) as user_total_order_conversions,
            sum(count_registration_conversions) over (
                partition by blended_user_id
                order by session_start_ts
                rows between unbounded preceding and current row
            ) as user_total_registration_conversions,
            sum(count_first_order_conversions) over (
                partition by blended_user_id
                order by session_start_ts
                rows between unbounded preceding and current row
            ) as user_total_first_order_conversions,
            sum(count_repeat_order_conversions) over (
                partition by blended_user_id
                order by session_start_ts
                rows between unbounded preceding and current row
            ) as user_total_repeat_order_conversions
        from converting_sessions_deduped_labelled
    ),
    converting_sessions_deduped_labelled_with_conversion_cycles as (
        select
            *,
            case
                when registration_conversion_event = 0
                then
                    max(coalesce(user_total_registration_conversions, 0)) over (
                        partition by blended_user_id
                        order by session_start_ts
                        rows between unbounded preceding and current row
                    )
                    + 1
                else
                    max(user_total_registration_conversions) over (
                        partition by blended_user_id
                        order by session_start_ts
                        rows between unbounded preceding and current row
                    )
            end as user_registration_conversion_cycle,

            case
                when conversion_event = 0
                then
                    max(coalesce(user_total_conversions, 0)) over (
                        partition by blended_user_id
                        order by session_start_ts
                        rows between unbounded preceding and current row
                    )
                    + 1
                else
                    max(user_total_conversions) over (
                        partition by blended_user_id
                        order by session_start_ts
                        rows between unbounded preceding and current row
                    )
            end as user_conversion_cycle,

            case
                when first_order_conversion_event = 0
                then
                    max(coalesce(user_total_first_order_conversions, 0)) over (
                        partition by blended_user_id
                        order by session_start_ts
                        rows between unbounded preceding and current row
                    )
                    + 1
                else
                    max(user_total_first_order_conversions) over (
                        partition by blended_user_id
                        order by session_start_ts
                        rows between unbounded preceding and current row
                    )
            end as user_first_order_conversion_cycle,

            case
                when repeat_order_conversion_event = 0
                then
                    max(coalesce(user_total_repeat_order_conversions, 0)) over (
                        partition by blended_user_id
                        order by session_start_ts
                        rows between unbounded preceding and current row
                    )
                    + 1
                else
                    max(user_total_repeat_order_conversions) over (
                        partition by blended_user_id
                        order by session_start_ts
                        rows between unbounded preceding and current row
                    )
            end as user_repeat_order_conversion_cycle
        from converting_sessions_deduped_labelled_with_conversion_number
    ),
    converting_sessions_deduped_labelled_with_session_day_number as (
        select
            *,

            {{ dbt_utils.datediff('"1900-01-01"', "session_start_ts", "day") }}
            as session_day_number
        from converting_sessions_deduped_labelled_with_conversion_cycles
    ),
    days_to_each_conversion as (
        select
            *,
            session_day_number - max(session_day_number) over (
                partition by blended_user_id, user_conversion_cycle
            ) as days_before_conversion,
            (
                session_day_number - max(session_day_number) over (
                    partition by blended_user_id, user_conversion_cycle
                )
            )
            * -1
            <= {{ var("attribution_lookback_days_window") }}
            as is_within_attribution_lookback_window,
            (
                session_day_number - max(session_day_number) over (
                    partition by blended_user_id, user_conversion_cycle
                )
            )
            * -1
            <= {{ var("attribution_time_decay_days_window") }}
            as is_within_attribution_time_decay_days_window
        from converting_sessions_deduped_labelled_with_session_day_number
    ),
    add_time_decay_score as (
        select
            *,
            if(
                is_within_attribution_time_decay_days_window,
                safe_divide
                (
                    pow(2, (days_before_conversion -1)),
                    ({{ var("attribution_time_decay_days_window") }})
                ),
                null
            ) as time_decay_score,
            if(
                conversion_session
                and not {{ var("attribution_include_conversion_session") }},
                0,
                pow(2, (days_before_conversion -1))
            ) as weighting,
            if(
                conversion_session
                and not {{ var("attribution_include_conversion_session") }},
                0,
                (
                    count
                    (
                        case
                            when
                                not conversion_session
                                or {{ var("attribution_include_conversion_session") }}
                            then session_id
                        end
                    ) over (
                        partition by
                            blended_user_id,
                            {{ dbt_utils.date_trunc("day", "session_start_ts") }}
                    )
                )
            ) as sessions_within_day_to_conversion,
            if(
                conversion_session
                and not {{ var("attribution_include_conversion_session") }},
                0,
                safe_divide
                (
                    pow(2, (days_before_conversion -1)),
                    count
                    (
                        case
                            when
                                not conversion_session
                                or {{ var("attribution_include_conversion_session") }}
                            then session_id
                        end
                    ) over (
                        partition by
                            blended_user_id,
                            {{ dbt_utils.date_trunc("day", "session_start_ts") }}
                    )
                )
            ) as weighting_split_by_days_sessions
        from days_to_each_conversion
    ),
    split_time_decay_score_across_days_sessions as (
        select
            *,
            safe_divide(
                time_decay_score, sessions_within_day_to_conversion
            ) as apportioned_time_decay_score
        from add_time_decay_score
    ),
    session_attrib_pct as (
        select
            * except (first_page_url_host),
            if(
                conversion_session
                and not {{ var("attribution_include_conversion_session") }},
                0,
                case
                    when
                        session_id
                        = last_value(
                            if(
                                is_within_attribution_lookback_window
                                and (
                                    not conversion_session
                                    or
                                    {{ var("attribution_include_conversion_session") }}
                                ),
                                session_id,
                                null
                            ) ignore nulls
                        ) over (
                            partition by blended_user_id, user_conversion_cycle
                            order by session_start_ts
                            rows between unbounded preceding and unbounded following
                        )

                    then 1
                    else 0
                end
            ) as last_click_attrib_pct,
            if(
                conversion_session
                and not {{ var("attribution_include_conversion_session") }},
                0,
                case
                    when
                        session_id
                        = last_value(
                            if(
                                is_within_attribution_lookback_window
                                and (
                                    not conversion_session
                                    or
                                    {{ var("attribution_include_conversion_session") }}
                                )
                                and is_non_direct_channel,
                                session_id,
                                null
                            ) ignore nulls
                        ) over (
                            partition by blended_user_id, user_conversion_cycle
                            order by session_start_ts
                            rows between unbounded preceding and unbounded following
                        )

                    then 1
                    else 0
                end
            ) as last_non_direct_click_attrib_pct,
            if(
                conversion_session
                and not {{ var("attribution_include_conversion_session") }},
                0,
                case
                    when
                        session_id
                        = last_value(
                            if(
                                is_within_attribution_lookback_window
                                and (
                                    not conversion_session
                                    or
                                    {{ var("attribution_include_conversion_session") }}
                                )
                                and is_paid_channel,
                                session_id,
                                null
                            ) ignore nulls
                        ) over (
                            partition by blended_user_id, user_conversion_cycle
                            order by session_start_ts
                            rows between unbounded preceding and unbounded following
                        )

                    then 1
                    else 0
                end
            ) as last_paid_click_attrib_pct,
            if(
                conversion_session
                and not {{ var("attribution_include_conversion_session") }},
                0,
                case
                    when
                        session_id
                        = first_value(
                            if(
                                is_within_attribution_lookback_window, session_id, null
                            ) ignore nulls
                        ) over (
                            partition by blended_user_id, user_conversion_cycle
                            order by session_start_ts
                            rows between unbounded preceding and unbounded following
                        )
                    then 1
                    else 0
                end
            ) as first_click_attrib_pct,
            if(
                conversion_session
                and not {{ var("attribution_include_conversion_session") }},
                0,
                case
                    when
                        session_id
                        = first_value(
                            if(
                                is_within_attribution_lookback_window
                                and (
                                    not conversion_session
                                    or
                                    {{ var("attribution_include_conversion_session") }}
                                )
                                and is_non_direct_channel,
                                session_id,
                                null
                            ) ignore nulls
                        ) over (
                            partition by blended_user_id, user_conversion_cycle
                            order by session_start_ts
                            rows between unbounded preceding and unbounded following
                        )
                    then 1
                    else 0
                end
            ) as first_non_direct_click_attrib_pct,
            if(
                conversion_session
                and not {{ var("attribution_include_conversion_session") }},
                0,
                case
                    when
                        session_id
                        = first_value(
                            if(
                                is_within_attribution_lookback_window
                                and (
                                    not conversion_session
                                    or
                                    {{ var("attribution_include_conversion_session") }}
                                )
                                and is_paid_channel,
                                session_id,
                                null
                            ) ignore nulls
                        ) over (
                            partition by blended_user_id, user_conversion_cycle
                            order by session_start_ts
                            rows between unbounded preceding and unbounded following
                        )
                    then 1
                    else 0
                end
            ) as first_paid_click_attrib_pct,
            if(
                conversion_session
                and not {{ var("attribution_include_conversion_session") }},
                0,
                if(
                    is_within_attribution_lookback_window,
                    (
                        safe_divide
                        (
                            1,
                            (
                                count
                                (
                                    if
                                    (
                                        is_within_attribution_lookback_window,
                                        session_id,
                                        null
                                    )
                                ) over (
                                    partition by blended_user_id, user_conversion_cycle
                                    order by session_start_ts
                                    rows between
                                        unbounded preceding and unbounded following
                                )
                                {% if var("attribution_include_conversion_session") %}
                                + 0
                                {% else %} -1
                                {% endif %}
                            )
                        )
                    ),
                    0
                )
            ) as even_click_attrib_pct,
            if(
                conversion_session
                and not {{ var("attribution_include_conversion_session") }},
                0,
                case
                    when is_within_attribution_time_decay_days_window
                    then
                        safe_divide(
                            apportioned_time_decay_score,
                            (
                                sum(apportioned_time_decay_score) over (
                                    partition by blended_user_id, user_conversion_cycle
                                )
                            )
                        )
                end
            ) as time_decay_attrib_pct
        from split_time_decay_score_across_days_sessions
    ),
    final as (
        select
            *,
            (
                max(count_registration_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * first_click_attrib_pct
            ) as user_registration_first_click_attrib_conversions,
            (
                max(count_registration_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * first_non_direct_click_attrib_pct
            ) as user_registration_first_non_direct_click_attrib_conversions,
            (
                max(count_registration_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * first_paid_click_attrib_pct
            ) as user_registration_first_paid_click_attrib_conversions,
            (
                max(count_registration_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * last_click_attrib_pct
            ) as user_registration_last_click_attrib_conversions,
            (
                max(count_registration_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * last_non_direct_click_attrib_pct
            ) as user_registration_last_non_direct_click_attrib_conversions,
            (
                max(count_registration_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * last_paid_click_attrib_pct
            ) as user_registration_last_paid_click_attrib_conversions,
            (
                max(count_registration_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * even_click_attrib_pct
            ) as user_registration_even_click_attrib_conversions,
            (
                max(count_registration_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * time_decay_attrib_pct
            ) as user_registration_time_decay_attrib_conversions,
            (
                max(count_first_order_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * first_click_attrib_pct
            ) as first_order_first_click_attrib_conversions,
            (
                max(count_first_order_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * first_non_direct_click_attrib_pct
            ) as first_order_first_non_direct_click_attrib_conversions,
            (
                max(count_first_order_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * first_paid_click_attrib_pct
            ) as first_order_first_paid_click_attrib_conversions,
            (
                max(count_first_order_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * last_click_attrib_pct
            ) as first_order_last_click_attrib_conversions,
            (
                max(count_first_order_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * last_non_direct_click_attrib_pct
            ) as first_order_last_non_direct_click_attrib_conversions,
            (
                max(count_first_order_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * last_paid_click_attrib_pct
            ) as first_order_last_paid_click_attrib_conversions,
            (
                max(count_first_order_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * even_click_attrib_pct
            ) as first_order_even_click_attrib_conversions,
            (
                max(count_first_order_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * time_decay_attrib_pct
            ) as first_order_time_decay_attrib_conversions,
            (
                max(first_order_total_revenue) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * first_click_attrib_pct
            ) as first_order_first_click_attrib_revenue,
            (
                max(first_order_total_revenue) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * first_non_direct_click_attrib_pct
            ) as first_order_first_non_direct_click_attrib_revenue,
            (
                max(first_order_total_revenue) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * first_paid_click_attrib_pct
            ) as first_order_first_paid_click_attrib_revenue,
            (
                max(first_order_total_revenue) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * last_click_attrib_pct
            ) as first_order_last_click_attrib_revenue,
            (
                max(first_order_total_revenue) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * last_non_direct_click_attrib_pct
            ) as first_order_last_non_direct_click_attrib_revenue,
            (
                max(first_order_total_revenue) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * last_paid_click_attrib_pct
            ) as first_order_last_paid_click_attrib_revenue,
            (
                max(first_order_total_revenue) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * even_click_attrib_pct
            ) as first_order_even_click_attrib_revenue,
            (
                max(first_order_total_revenue) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * time_decay_attrib_pct
            ) as first_order_time_decay_attrib_revenue,
            (
                max(count_repeat_order_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * first_click_attrib_pct
            ) as repeat_order_first_click_attrib_conversions,
            (
                max(count_repeat_order_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * first_non_direct_click_attrib_pct
            ) as repeat_order_first_non_direct_click_attrib_conversions,
            (
                max(count_repeat_order_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * first_paid_click_attrib_pct
            ) as repeat_order_first_paid_click_attrib_conversions,
            (
                max(count_repeat_order_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * last_click_attrib_pct
            ) as repeat_order_last_click_attrib_conversions,
            (
                max(count_repeat_order_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * last_non_direct_click_attrib_pct
            ) as repeat_order_last_non_direct_click_attrib_conversions,
            (
                max(count_repeat_order_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * last_paid_click_attrib_pct
            ) as repeat_order_last_paid_click_attrib_conversions,
            (
                max(count_repeat_order_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * even_click_attrib_pct
            ) as repeat_order_even_click_attrib_conversions,
            (
                max(count_repeat_order_conversions) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * time_decay_attrib_pct
            ) as repeat_order_time_decay_attrib_conversions,
            (
                max(repeat_order_total_revenue) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * first_click_attrib_pct
            ) as repeat_order_first_click_attrib_revenue,
            (
                max(repeat_order_total_revenue) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * first_non_direct_click_attrib_pct
            ) as repeat_order_first_non_direct_click_attrib_revenue,
            (
                max(repeat_order_total_revenue) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * first_paid_click_attrib_pct
            ) as repeat_order_first_paid_click_attrib_revenue,
            (
                max(repeat_order_total_revenue) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * last_click_attrib_pct
            ) as repeat_order_last_click_attrib_revenue,
            (
                max(repeat_order_total_revenue) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * last_non_direct_click_attrib_pct
            ) as repeat_order_last_non_direct_click_attrib_revenue,
            (
                max(repeat_order_total_revenue) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * last_paid_click_attrib_pct
            ) as repeat_order_last_paid_click_attrib_revenue,
            (
                max(repeat_order_total_revenue) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * even_click_attrib_pct
            ) as repeat_order_even_click_attrib_revenue,
            (
                max(repeat_order_total_revenue) over (
                    partition by blended_user_id, user_conversion_cycle
                )
                * time_decay_attrib_pct
            ) as repeat_order_time_decay_attrib_revenue
        from session_attrib_pct {{ dbt_utils.group_by(57) }}
    )
select
    blended_user_id,
    session_start_ts,
    session_end_ts,
    session_id,
    session_seq,
    conversion_session,
    utm_source,
    utm_content,
    utm_medium,
    utm_campaign,
    referrer_host,
    referrer_domain,
    channel,
    first_order_total_revenue,
    repeat_order_total_revenue,
    currency_code,
    user_conversion_cycle,
    user_registration_conversion_cycle,
    user_first_order_conversion_cycle,
    user_repeat_order_conversion_cycle,
    is_within_attribution_lookback_window,
    is_within_attribution_time_decay_days_window,
    is_non_direct_channel,
    is_paid_channel,
    sessions_within_day_to_conversion,
    time_decay_score,
    apportioned_time_decay_score,
    days_before_conversion,
    weighting as time_decay_score_weighting,
    weighting_split_by_days_sessions as time_decay_weighting_split_by_days_sessions,
    count_conversions,
    count_order_conversions,
    count_first_order_conversions,
    count_repeat_order_conversions,
    count_registration_conversions,
    first_click_attrib_pct,
    first_non_direct_click_attrib_pct,
    first_paid_click_attrib_pct,
    last_click_attrib_pct,
    last_non_direct_click_attrib_pct,
    last_paid_click_attrib_pct,
    even_click_attrib_pct,
    time_decay_attrib_pct,
    user_registration_first_click_attrib_conversions,
    user_registration_first_non_direct_click_attrib_conversions,
    user_registration_first_paid_click_attrib_conversions,
    user_registration_last_click_attrib_conversions,
    user_registration_last_non_direct_click_attrib_conversions,
    user_registration_last_paid_click_attrib_conversions,
    user_registration_even_click_attrib_conversions,
    user_registration_time_decay_attrib_conversions,
    first_order_first_click_attrib_conversions,
    first_order_first_non_direct_click_attrib_conversions,
    first_order_first_paid_click_attrib_conversions,
    first_order_last_click_attrib_conversions,
    first_order_last_non_direct_click_attrib_conversions,
    first_order_last_paid_click_attrib_conversions,
    first_order_even_click_attrib_conversions,
    first_order_time_decay_attrib_conversions,
    first_order_first_click_attrib_revenue,
    first_order_first_non_direct_click_attrib_revenue,
    first_order_first_paid_click_attrib_revenue,
    first_order_last_click_attrib_revenue,
    first_order_last_non_direct_click_attrib_revenue,
    first_order_last_paid_click_attrib_revenue,
    first_order_even_click_attrib_revenue,
    first_order_time_decay_attrib_revenue,
    repeat_order_first_click_attrib_conversions,
    repeat_order_first_non_direct_click_attrib_conversions,
    repeat_order_first_paid_click_attrib_conversions,
    repeat_order_last_click_attrib_conversions,
    repeat_order_last_non_direct_click_attrib_conversions,
    repeat_order_last_paid_click_attrib_conversions,
    repeat_order_even_click_attrib_conversions,
    repeat_order_time_decay_attrib_conversions,
    repeat_order_first_click_attrib_revenue,
    repeat_order_first_non_direct_click_attrib_revenue,
    repeat_order_first_paid_click_attrib_revenue,
    repeat_order_last_click_attrib_revenue,
    repeat_order_last_non_direct_click_attrib_revenue,
    repeat_order_last_paid_click_attrib_revenue,
    repeat_order_even_click_attrib_revenue,
    repeat_order_time_decay_attrib_revenue
from final
{% endif %}
