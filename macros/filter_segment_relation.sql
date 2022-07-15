{%- macro filter_segment_relation(relation) -%}

select *
from
    (
        select
            *,
            max(uuid_ts) over (
                partition by id
                order by
                    uuid_ts range between unbounded preceding and unbounded following
            ) as max_uuid_ts
        from {{ relation }}
    )
where uuid_ts = max_uuid_ts
{%- endmacro -%}
