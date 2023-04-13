{% if var("finance_warehouse_payment_sources") %}

    with
        t_currencies_merge_list as (

            {% for source in var("finance_warehouse_payment_sources") %}
                {% set relation_source = "stg_" + source + "_currencies" %}

                select '{{source}}' as source, *
                from {{ ref(relation_source) }}

                {% if not loop.last %}
                    union all
                {% endif %}
            {% endfor %}
        )
    select *
    from t_currencies_merge_list

{% else %} {{ config(enabled=false) }}
{% endif %}
