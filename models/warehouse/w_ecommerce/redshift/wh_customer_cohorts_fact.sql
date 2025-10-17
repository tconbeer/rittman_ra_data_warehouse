{% if var("ecommerce_warehouse_customer_cohorts_sources") %}

    {{ config(unique_key="customer_cohort_pk", alias="customer_cohorts_fact") }}

    with
        customer_cohorts as (select * from {{ ref("int_customer_cohorts") }} o),
        customers as (select * from {{ ref("wh_customers_dim") }} o)
    select
        {{ dbt_utils.surrogate_key(["h.customer_id", "h.date_month"]) }}
        as customer_cohort_pk,
        h.*
    from customer_cohorts h
    left join customers c on h.customer_id = c.customer_id

{% else %} {{ config(enabled=false) }}
{% endif %}
