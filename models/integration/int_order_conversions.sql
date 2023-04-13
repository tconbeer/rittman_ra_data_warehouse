{% if var("order_conversion_sources") %}

    with
        t_order_conversions as (

            {% for source in var("order_conversion_sources") %}
                {% set relation_source = "stg_" + source + "_order_conversions" %}

                select '{{source}}' as source, *
                from {{ ref(relation_source) }}

                {% if not loop.last %}
                    union all
                {% endif %}
            {% endfor %}
        )
    select *
    from t_order_conversions

{% else %} {{ config(enabled=false) }}
{% endif %}
