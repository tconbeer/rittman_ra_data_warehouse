{% if target.type == "bigquery" %}
    {% if var("crm_warehouse_conversations_sources") %}
        {% if "hubspot_crm" in var("crm_warehouse_conversations_sources") %}
            {% if var("stg_hubspot_crm_etl") == "stitch" %}

                with
                    source as (
                        {{
                            filter_stitch_relation(
                                relation=source("stitch_hubspot_crm", "engagements"),
                                unique_column="engagement_id",
                            )
                        }}
                    ),
                    renamed as (
                        select
                            concat(
                                '{{ var(' stg_hubspot_crm_id - prefix ') }}',
                                engagement_id
                            ) as conversation_id,
                            concat(
                                '{{ var(' stg_hubspot_crm_id - prefix ') }}',
                                contactids.value
                            ) as conversation_user_id,
                            concat(
                                '{{ var(' stg_hubspot_crm_id - prefix ') }}',
                                contactids.value
                            ) as conversation_author_id,
                            concat(
                                '{{ var(' stg_hubspot_crm_id - prefix ') }}',
                                companyids.value
                            ) as company_id,
                            cast(
                                null as {{ dbt_utils.type_string() }}
                            ) as conversation_author_type,
                            cast(
                                null as {{ dbt_utils.type_string() }}
                            ) as conversation_user_type,
                            concat(
                                '{{ var(' stg_hubspot_crm_id - prefix ') }}',
                                contactids.value
                            ) as conversation_assignee_id,
                            cast(
                                null as {{ dbt_utils.type_string() }}
                            ) as conversation_assignee_state,
                            concat(
                                '{{ var(' stg_hubspot_crm_id - prefix ') }}',
                                engagement_id
                            ) as conversation_message_id,
                            coalesce(
                                engagement.type,
                                cast(null as {{ dbt_utils.type_string() }})
                            ) as conversation_message_type,
                            coalesce(
                                metadata.text,
                                cast(null as {{ dbt_utils.type_string() }})
                            ) as conversation_body,
                            coalesce(
                                metadata.subject,
                                cast(null as {{ dbt_utils.type_string() }})
                            ) as conversation_subject,
                            engagement.createdat as conversation_created_date,
                            engagement.lastupdated as contact_last_modified_date,
                            cast(
                                null as {{ dbt_utils.type_boolean() }}
                            ) as is_conversation_read,
                            cast(
                                null as {{ dbt_utils.type_boolean() }}
                            ) as is_conversation_open,
                            dealids.value as deal_id
                        from
                            source,
                            unnest(associations.contactids) as contactids,
                            unnest(associations.companyids) as companyids,
                            unnest(associations.dealids) as dealids,
                            unnest(metadata.to) as metadata_to
                            {{ dbt_utils.group_by(n=17) }}
                    )
                select *
                from renamed

            {% else %} {{ config(enabled=false) }}
            {% endif %}
        {% else %} {{ config(enabled=false) }}
        {% endif %}
    {% else %} {{ config(enabled=false) }}
    {% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
