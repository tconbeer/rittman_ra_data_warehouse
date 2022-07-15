{{ config(enabled=target.type == "bigquery") }}
{% if var("crm_warehouse_conversations_sources") %}
{% if "intercom_messaging" in var("crm_warehouse_conversations_sources") %}

with
    source as (
        {{
            filter_stitch_relation(
                relation=source("stitch_intercom_messaging", "conversations"),
                unique_column="id",
            )
        }}
    ),
    renamed as (
        select
            concat(
                '{{ var(' stg_intercom_messaging_id - prefix ') }}', id
            ) as conversation_id,
            concat(
                '{{ var(' stg_intercom_messaging_id - prefix ') }}', user.id
            ) as conversation_user_id,
            concat(
                '{{ var(' stg_intercom_messaging_id - prefix ') }}',
                conversation_message.author.id
            ) as conversation_author_id,
            cast(null as {{ dbt_utils.type_string() }}) as company_id,
            conversation_message.author.type as conversation_author_type,
            user.type as conversation_user_type,
            concat(
                '{{ var(' stg_intercom_messaging_id - prefix ') }}', assignee.id
            ) as conversation_assignee_id,
            assignee.type as conversation_assignee_state,
            concat(
                '{{ var(' stg_intercom_messaging_id - prefix ') }}',
                conversation_message.id
            ) as conversation_message_id,
            conversation_message.type as conversation_message_type,
            conversation_message.body as conversation_body,
            conversation_message.subject as conversation_subject,
            created_at as contact_created_date,
            updated_at as contact_last_modified_date,
            read as is_conversation_read,
            open as is_conversation_open,
            null as deal_id
        from source
    )
select *
from renamed

{% else %} {{ config(enabled=false) }}
{% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
