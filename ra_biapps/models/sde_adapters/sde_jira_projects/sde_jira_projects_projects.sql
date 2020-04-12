{% if not enable_jira_projects %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with projects as (SELECT
  *
  FROM (
  SELECT
    'jira_projects'               as source,
    concat('jira-',id) as project_id,
    concat('jira-',lead.key) as lead_user_id,
    name as project_name,
    projectkeys as jira_project_key,
    projecttypekey as project_type_id,
    cast (null as string) as project_status,
    cast (null as string) as project_notes,

    projectcategory.id as project_category_id,
    _sdc_batched_at,
    MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
  FROM
    {{ source('jira', 'projects') }})
WHERE
  _sdc_batched_at = max_sdc_batched_at),
  types as (SELECT
    *
    FROM (
    SELECT
      key as project_type_id,
      formattedKey as project_type,
        _sdc_batched_at,
      MAX(_sdc_batched_at) OVER (PARTITION BY key ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
    FROM
      {{ source('jira', 'project_types') }})
  WHERE
    _sdc_batched_at = max_sdc_batched_at),
    categories as (SELECT
      *
      FROM (
      SELECT
        id as project_category_id,
        description as project_category_description,
        name as project_category_name,
          _sdc_batched_at,
        MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
      FROM
        {{ source('jira', 'project_categories') }})
    WHERE
      _sdc_batched_at = max_sdc_batched_at)
select p.source,
       p.project_id,
       p.lead_user_id,
       p.project_name,
       p.project_status,
       p.project_notes,
       t.project_type as project_type,
       c.project_category_description,
       c.project_category_name,
       cast (null as timestamp) as project_created_at_ts,
       cast (null as timestamp) as project_modified_at_ts

from projects p
join types t on p.project_type_id = t.project_type_id
join categories c on p.project_category_id = c.project_category_id
