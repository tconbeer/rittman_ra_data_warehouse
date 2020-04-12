{% if not enable_asana_projects %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  SELECT
    * EXCEPT (_sdc_batched_at, max_sdc_batched_at)
  FROM
    (
      SELECT
        *,
        MAX(_sdc_batched_at) OVER (PARTITION BY gid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
      FROM
        {{ source('stitch_asana','projects') }}
    )
  WHERE
    _sdc_batched_at = max_sdc_batched_at
),
renamed AS (
  SELECT
  'asana_projects' as source,
  concat('asana-',gid) as project_id,
  concat('asana-',owner.gid) as lead_user_id,
  name as project_name,
  current_status as project_status,
  notes as project_notes,
  cast (null as string) as project_type,
  cast (null as string) as project_category_description,
  cast (null as string) as project_category_name,
  created_at as project_created_at_ts,
  modified_at as project_modified_at_ts,
  FROM
    source
)
SELECT
  *
FROM
  renamed
