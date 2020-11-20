{% if not var("enable_hubspot_crm_source")  %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("stg_hubspot_crm_etl") == 'fivetran' %}
with source as (
  select * from
  from {{ target.database}}.{{ var('stg_hubspot_crm_fivetran_schema') }}.{{ var('stg_hubspot_crm_fivetran_engagements_table') }}

),
SELECT
  engagement_id                     as message_id,
  companyids.value                  as company_id,
  contactids.value                  as contact_id,
  engagement.createdat              as message_created_ts,
  engagement.type                   as message_type_ts,
  engagement.ownerid                as owner_id,
  dealids.value                     as deal_id,
  engagement.timestamp              as emessage_ts,
  metadata.status                   as emessage_status,
  metadata.from.firstname           as message_from_first_name,
  metadata.from.lastname            as message_from_last_name,
  metadata.from.email               as message_from_email,
  metadata.title                    as message_title,
  metadata.subject                  as message_subject,
  metadata_to.value.email           as message_to_email,
  metadata.text                     as message_text, 
  engagement.lastupdated            as message_lastupdated,
FROM
  source,
  unnest(associations.contactids) as contactids,
  unnest(associations.companyids) as companyids,
  unnest(associations.dealids) as dealids,
  unnest(metadata.to) as metadata_to
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)
{% elif var("stg_hubspot_crm_etl") == 'stitch' %}
with source as (
  {{ filter_stitch_table(var('stg_hubspot_crm_stitch_schema'),var('stg_hubspot_crm_stitch_engagements_table'),'id') }}

),
SELECT
  engagement_id                     as message_id,
  companyids.value                  as company_id,
  contactids.value                  as contact_id,
  engagement.createdat              as message_created_ts,
  engagement.type                   as message_type_ts,
  engagement.ownerid                as owner_id,
  dealids.value                     as deal_id,
  engagement.timestamp              as emessage_ts,
  metadata.status                   as emessage_status,
  metadata.from.firstname           as message_from_first_name,
  metadata.from.lastname            as message_from_last_name,
  metadata.from.email               as message_from_email,
  metadata.title                    as message_title,
  metadata.subject                  as message_subject,
  metadata_to.value.email           as message_to_email,
  metadata.text                     as message_text, 
  engagement.lastupdated            as message_lastupdated,
FROM
  source,
  unnest(associations.contactids) as contactids,
  unnest(associations.companyids) as companyids,
  unnest(associations.dealids) as dealids,
  unnest(metadata.to) as metadata_to
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
{% endif %}
select * from renamed
