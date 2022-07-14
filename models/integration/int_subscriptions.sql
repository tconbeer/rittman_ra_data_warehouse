{% if var("subscriptions_warehouse_sources") %}
with
    subscriptions_merge_list as
    (select * from {{ ref("stg_stripe_subscriptions_subscriptions") }}
    )
select *
from subscriptions_merge_list

{% else %} {{ config(enabled=false) }}


{% endif %}
