{% if var("subscriptions_warehouse_sources") %}

{{ config(alias="attribution_fact") }}

with
    subscription_details as
    (
        select {{ dbt_utils.star(from=ref("int_converter_subscription_revenue")) }}
        from {{ ref("int_converter_subscription_revenue") }}
    ),
    converting_events as
    (
        select
            e.blended_user_id,
            first_value(
                case
                    when event_type = '{{ var(' attribution_conversion_event_type ') }}'
                    then session_id
                end
            ) over (
                partition by e.blended_user_id
                order by e.event_ts
                rows between unbounded preceding and current row
            ) as session_id,
            s.* except (event_id, user_id),
            event_type,
            min(
                case
                    when event_type = '{{ var(' attribution_conversion_event_type ') }}'
                    then event_ts
                end
            ) over (partition by e.blended_user_id) as converted_ts,
            min(
                case
                    when
                        event_type
                        = '{{ var(' attribution_create_account_event_type ') }}'
                    then event_ts
                end
            ) over (partition by e.blended_user_id) as created_account_ts
        from {{ ref("wh_web_events_fact") }} e
        left outer join subscription_details s on e.event_id = s.event_id
        where
            event_type = '{{ var(' attribution_conversion_event_type ') }}'
            or event_type = '{{ var(' attribution_create_account_event_type ') }}'
    ),
    converting_sessions as (
        select * from converting_events {{ dbt_utils.group_by(12) }}
    ),
    converting_sessions_deduped as (
        select
            blended_user_id as blended_user_id,
            max(
                case
                    when event_type = '{{ var(' attribution_conversion_event_type ') }}'
                    then session_id
                end
            ) as session_id,
            max(plan_id) as plan_id,
            min(plan_name) as plan_name,
            min(plan_interval_count) as plan_interval_count,
            min(plan_amount) as plan_amount,
            max(baremetrics_predicted_ltv) as baremetrics_predicted_ltv,
            min(converted_ts) as converted_ts,
            min(created_account_ts) as created_account_ts
        from converting_sessions
        group by 1
    ),
    converting_sessions_deduped_labelled as
    (
        select
            c.blended_user_id,
            max(c.plan_amount) over (partition by c.blended_user_id) as plan_amount,
            max(c.baremetrics_predicted_ltv) over (
                partition by c.blended_user_id
            ) as baremetrics_predicted_ltv,
            s.session_start_ts,
            s.session_end_ts,
            c.converted_ts,
            c.created_account_ts,
            s.session_id as session_id,
            row_number() over (
                partition by c.blended_user_id order by s.session_start_ts
            ) as session_seq,
            case
                when
                    c.created_account_ts between s.session_start_ts and s.session_end_ts
                then true
                else false
            end as account_opening_session,
            case
                when (c.converted_ts between s.session_start_ts and s.session_end_ts)
                then true
                else false
            end as conversion_session,
            case
                when (c.converted_ts between s.session_start_ts and s.session_end_ts)
                then 1
                else 0
            end as event,
            case
                when
                    s.session_start_ts between c.created_account_ts and coalesce(
                        c.converted_ts, s.session_end_ts
                    )
                then true
                else false
            end as trialing_session,
            utm_source,
            utm_content,
            utm_medium,
            utm_campaign,
            referrer_host,
            first_page_url_host,
            split(net.reg_domain(referrer_host), '.') [offset (0)] as referrer_domain,
            channel,
            events
        from {{ ref("wh_web_sessions_fact") }} s
        join converting_sessions_deduped c on c.blended_user_id = s.blended_user_id
        where c.converted_ts >= s.session_start_ts
        order by c.blended_user_id, s.session_start_ts
    ),
    session_attrib_pct as (
        select
            * except (first_page_url_host),
            case
                when
                    session_id = last_value(
                        session_id
                    ) over (
                        partition by blended_user_id
                        order by session_start_ts
                        rows between unbounded preceding and unbounded following
                    )
                then 1
                else 0
            end as last_click_attrib_pct,
            case
                when
                    session_id = first_value(
                        session_id
                    ) over (
                        partition by blended_user_id
                        order by session_start_ts
                        rows between unbounded preceding and unbounded following
                    )
                then 1
                else 0
            end as first_click_attrib_pct,
            1 / count(
                session_id
            ) over (
                partition by blended_user_id
            ) as even_click_attrib_pct,
            case
                when
                    session_start_ts = first_value(
                        session_start_ts
                    ) over (
                        partition by blended_user_id order by session_start_ts
                    )
                    and max(event) over (partition by blended_user_id) = 1
                then
                    safe_cast(
                        1.1 - row_number() over (partition by blended_user_id) as string
                    )
                when
                    session_start_ts > lag(
                        session_start_ts
                    ) over (
                        partition by blended_user_id order by session_start_ts
                    )
                    and max(event) over (partition by blended_user_id) = 1
                then
                    safe_cast(
                        round(
                            1.1 -1 / row_number() over (partition by blended_user_id), 2
                        ) as string
                    )
                else 'null'
            end as weights
        from converting_sessions_deduped_labelled
    ),
    session_attrib_pct_with_time_decay as (
        select
            * except (weights),
            round(
                if(
                    safe_cast(weights as float64) = 0
                    or sum(safe_cast(weights as float64)) over (
                        partition by blended_user_id
                    )
                    = 0,
                    0,
                    safe_cast(weights as float64) / sum(
                        safe_cast(weights as float64)
                    ) over (
                        partition by blended_user_id
                    )
                ),
                2
            ) as time_decay_attrib_pct
        from session_attrib_pct
    ),
    final as (
        select
            *,
            round(
                max(plan_amount * first_click_attrib_pct), 2
            ) as first_click_attrib_first_plan,
            round(
                max(plan_amount * last_click_attrib_pct), 2
            ) as last_click_attrib_first_plan,
            round(
                max(plan_amount * even_click_attrib_pct), 2
            ) as even_click_attrib_first_plan,
            round(
                max(plan_amount * time_decay_attrib_pct), 2
            ) as time_decay_attrib_first_plan,
            round(
                max(baremetrics_predicted_ltv * first_click_attrib_pct), 2
            ) as first_click_attrib_baremetrics_predicted_ltv,
            round(
                max(baremetrics_predicted_ltv * last_click_attrib_pct), 2
            ) as last_click_attrib_baremetrics_predicted_ltv,
            round(
                max(baremetrics_predicted_ltv * even_click_attrib_pct), 2
            ) as even_click_attrib_baremetrics_predicted_ltv,
            round(
                max(baremetrics_predicted_ltv * time_decay_attrib_pct), 2
            ) as time_decay_attrib_baremetrics_predicted_ltv
        from session_attrib_pct_with_time_decay {{ dbt_utils.group_by(25) }}
    )
select *
from final

{% else %} {{ config(enabled=false) }}

{% endif %}
