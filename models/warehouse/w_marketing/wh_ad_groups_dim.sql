{% if var("marketing_warehouse_ad_group_sources") %}

    {{ config(unique_key="ad_group_pk", alias="ad_groups_dim") }}

    with ad_groups as (select * from {{ ref("int_ad_groups") }})
    select {{ dbt_utils.surrogate_key(["ad_group_id"]) }} as ad_group_pk, a.*
    from ad_groups a

{% else %} {{ config(enabled=false) }}
{% endif %}
