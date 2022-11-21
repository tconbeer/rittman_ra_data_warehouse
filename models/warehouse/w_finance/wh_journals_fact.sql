{% if var("finance_warehouse_journal_sources") %}

{{ config(alias="journals_fact", unique_key="journal_pk", materialized="table") }}


with journals as (select * from {{ ref("int_journals") }})

select {{ dbt_utils.surrogate_key(["journal_id", "journal_line_id"]) }} as journal_pk, *
from journals

{% else %} {{ config(enabled=false) }}
{% endif %}
