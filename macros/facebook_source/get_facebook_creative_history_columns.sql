{% macro get_facebook_creative_history_columns() %}

    {% set columns = [
        {"name": "_fivetran_id", "datatype": dbt_utils.type_string()},
        {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()},
        {"name": "account_id", "datatype": dbt_utils.type_int()},
        {"name": "actor_id", "datatype": dbt_utils.type_int()},
        {"name": "applink_treatment", "datatype": dbt_utils.type_string()},
        {
            "name": "asset_feed_spec_link_urls",
            "datatype": dbt_utils.type_string(),
        },
        {"name": "body", "datatype": dbt_utils.type_string()},
        {
            "name": "branded_content_sponsor_page_id",
            "datatype": dbt_utils.type_int(),
        },
        {"name": "call_to_action_type", "datatype": dbt_utils.type_string()},
        {"name": "carousel_ad_link", "datatype": dbt_utils.type_string()},
        {
            "name": "effective_instagram_story_id",
            "datatype": dbt_utils.type_int(),
        },
        {
            "name": "effective_object_story_id",
            "datatype": dbt_utils.type_string(),
        },
        {"name": "id", "datatype": dbt_utils.type_int()},
        {"name": "image_file", "datatype": dbt_utils.type_string()},
        {"name": "image_hash", "datatype": dbt_utils.type_string()},
        {"name": "image_url", "datatype": dbt_utils.type_string()},
        {"name": "instagram_actor_id", "datatype": dbt_utils.type_int()},
        {
            "name": "instagram_permalink_url",
            "datatype": dbt_utils.type_string(),
        },
        {"name": "instagram_story_id", "datatype": dbt_utils.type_int()},
        {"name": "link_og_id", "datatype": dbt_utils.type_int()},
        {"name": "link_url", "datatype": dbt_utils.type_string()},
        {"name": "name", "datatype": dbt_utils.type_string()},
        {"name": "object_id", "datatype": dbt_utils.type_int()},
        {"name": "object_story_id", "datatype": dbt_utils.type_string()},
        {
            "name": "object_story_link_data_app_link_spec_android",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "object_story_link_data_app_link_spec_ios",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "object_story_link_data_app_link_spec_ipad",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "object_story_link_data_app_link_spec_iphone",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "object_story_link_data_caption",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "object_story_link_data_child_attachments",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "object_story_link_data_description",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "object_story_link_data_link",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "object_story_link_data_message",
            "datatype": dbt_utils.type_string(),
        },
        {"name": "object_type", "datatype": dbt_utils.type_string()},
        {"name": "object_url", "datatype": dbt_utils.type_string()},
        {"name": "page_link", "datatype": dbt_utils.type_string()},
        {"name": "page_message", "datatype": dbt_utils.type_string()},
        {"name": "product_set_id", "datatype": dbt_utils.type_int()},
        {"name": "status", "datatype": dbt_utils.type_string()},
        {
            "name": "template_app_link_spec_android",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "template_app_link_spec_ios",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "template_app_link_spec_ipad",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "template_app_link_spec_iphone",
            "datatype": dbt_utils.type_string(),
        },
        {"name": "template_caption", "datatype": dbt_utils.type_string()},
        {
            "name": "template_child_attachments",
            "datatype": dbt_utils.type_string(),
        },
        {"name": "template_description", "datatype": dbt_utils.type_string()},
        {"name": "template_link", "datatype": dbt_utils.type_string()},
        {"name": "template_message", "datatype": dbt_utils.type_string()},
        {"name": "template_page_link", "datatype": dbt_utils.type_string()},
        {"name": "template_url", "datatype": dbt_utils.type_string()},
        {"name": "thumbnail_url", "datatype": dbt_utils.type_string()},
        {"name": "title", "datatype": dbt_utils.type_string()},
        {"name": "url_tags", "datatype": dbt_utils.type_string()},
        {"name": "use_page_actor_override", "datatype": "boolean"},
        {
            "name": "video_call_to_action_value_link",
            "datatype": dbt_utils.type_string(),
        },
        {"name": "video_id", "datatype": dbt_utils.type_int()},
    ] %}

    {{ return(columns) }}

{% endmacro %}
