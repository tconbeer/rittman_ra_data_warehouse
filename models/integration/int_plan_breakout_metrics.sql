{% if var("subscriptions_warehouse_sources") %}
with
    plans_breakout_merge_list as
    (select * from {{ ref("stg_baremetrics_plan_breakout") }}
    )
select *
from plans_breakout_merge_list

{% else %} {{ config(enabled=false) }}


{% endif %}
