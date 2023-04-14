{%- macro filter_stitch_relation(relation, unique_column) -%}

    select *
    from
        (
            select
                *,
                max(_sdc_batched_at) over (
                    partition by {{ unique_column }}
                    order by
                        _sdc_batched_at range
                        between unbounded preceding and unbounded following
                ) as max_sdc_batched_at
            from {{ relation }}
        )
    where _sdc_batched_at = max_sdc_batched_at

{%- endmacro -%}
