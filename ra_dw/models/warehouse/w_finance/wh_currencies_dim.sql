{% if not var("enable_finance_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        unique_key='account_pk',
        alias='currency_dim'
    )
}}
{% endif %}

WITH currencies AS
  (
  SELECT *
  FROM   {{ ref('int_currencies') }}
  )

SELECT
   GENERATE_UUID() as account_pk,
   *
FROM
   currencies
