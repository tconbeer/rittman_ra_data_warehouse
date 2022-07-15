{%- macro conversion_orders() -%}

create table if
not exists {{ target.database }}.{{ target.schema }}_staging.conversion_orders(
    order_id string,
    customer_id string,
    order_ts timestamp,
    session_id string,
    checkout_id string,
    total_revenue float64,
    currency_code string,
    utm_source string,
    utm_medium string,
    utm_campaign string,
    utm_content string,
    utm_term string,
    channel string,
    last_updated_at_ts timestamp
)
;
{% endmacro %}
