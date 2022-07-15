{%- macro profile_schema(table_schema) -%}

{{ config(schema="profiles") }}

{% set not_null_profile_threshold_pct = ".9" %}
{% set unique_profile_threshold_pct = ".9" %}

{% set tables = dbt_utils.get_relations_by_prefix(table_schema, "") %}

select
    column_stats.table_catalog,
    column_stats.table_schema,
    column_stats.table_name,
    column_stats.column_name,
    case
        when column_metadata.is_nullable = 'YES' then false else true
    end as is_not_nullable_column,
    case
        when column_stats.pct_not_null > {{ not_null_profile_threshold_pct }}
        then true
        else false
    end as is_recommended_not_nullable_column,

    column_stats._nulls as count_nulls,
    column_stats._non_nulls as count_not_nulls,
    column_stats.pct_not_null as pct_not_null,
    column_stats.table_rows,
    column_stats.count_distinct_values,
    column_stats.pct_unique,
    case
        when column_stats.pct_unique >= {{ unique_profile_threshold_pct }}
        then true
        else false
    end as is_recommended_unique_column,

    column_metadata.* except (
        table_catalog, table_schema, table_name, column_name, is_nullable
    ),
    column_stats.* except (
        table_catalog,
        table_schema,
        table_name,
        column_name,
        _nulls,
        _non_nulls,
        pct_not_null,
        table_rows,
        pct_unique,
        count_distinct_values
    )
from
    (
        {% for table in tables %}
        select *
        from
            (
                with
                    `table` as (select * from {{ table }}),
                    table_as_json as (
                        select regexp_replace(to_json_string(t), r'^{|}$', '') as row
                        from `table` as t
                    ),
                    pairs as (
                        select
                            replace(column_name, '"', '') as column_name,
                            if(
                                safe_cast(column_value as string) = 'null',
                                null,
                                column_value
                            ) as column_value
                        from
                            table_as_json,
                            unnest(split(row, ',"')) as z,
                            unnest( [split(z, ':') [safe_offset(0)]]) as column_name,
                            unnest( [split(z, ':') [safe_offset(1)]]) as column_value
                    ),
                    profile as (
                        select
                            split(
                                replace('{{ table }}', '`', ''), '.') [safe_offset(0)
                            ] as table_catalog,
                            split(
                                replace('{{ table }}', '`', ''), '.') [safe_offset(1)
                            ] as table_schema,
                            split(
                                replace('{{ table }}', '`', ''), '.') [safe_offset(2)
                            ] as table_name,
                            column_name,
                            count(*) as table_rows,
                            count(distinct column_value) as count_distinct_values,
                            safe_divide(
                                count(distinct column_value), count(*)
                            ) as pct_unique,
                            countif(column_value is null) as _nulls,
                            countif(column_value is not null) as _non_nulls,
                            countif(column_value is not null)
                            / count(*) as pct_not_null,
                            min(column_value) as _min_value,
                            max(column_value) as _max_value,
                            avg(safe_cast(column_value as numeric)) as _avg_value,
                            approx_top_count(
                                column_value, 1) [offset (0)
                            ] as _most_frequent_value,
                            min(
                                length(safe_cast(column_value as string))
                            ) as _min_length,
                            max(
                                length(safe_cast(column_value as string))
                            ) as _max_length,
                            round(
                                avg(length(safe_cast(column_value as string)))
                            ) as _avr_length
                        from pairs
                        where column_name <> '' and column_name not like '%-%'
                        group by column_name
                        order by column_name
                    )
                select *
                from profile
            )
        {%- if not loop.last %}
        union all
        {%- endif %}
        {% endfor %}
    ) column_stats
left outer join
    (
        select * except (is_generated, generation_expression, is_stored, is_updatable)
        from {{ table_schema }}.information_schema.columns
    ) column_metadata
    on column_stats.table_catalog = column_metadata.table_catalog
    and column_stats.table_schema = column_metadata.table_schema
    and column_stats.table_name = column_metadata.table_name
    and column_stats.column_name = column_metadata.column_name

{%- endmacro -%}
