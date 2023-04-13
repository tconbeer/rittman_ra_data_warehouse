{% if var("crm_warehouse_contact_sources") %}

    {{ config(unique_key="contact_pk", alias="contacts_dim") }}

    with contacts as (select * from {{ ref("int_contacts") }} c)
    select {{ dbt_utils.surrogate_key(["contact_name"]) }} as contact_pk, *
    from contacts c

{% else %} {{ config(enabled=false) }}
{% endif %}
