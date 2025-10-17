{% if var("crm_warehouse_company_sources") %}

    {{ config(alias="companies_dim") }}

    with
        companies_dim as (
            select {{ dbt_utils.surrogate_key(["company_name"]) }} as company_pk, *
            from {{ ref("int_companies") }} c
        )
    select *
    from companies_dim

{% else %} {{ config(enabled=false) }}
{% endif %}
