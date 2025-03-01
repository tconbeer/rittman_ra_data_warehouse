{% if var("marketing_warehouse_ad_sources") %}
    {% if "facebook_ads" in var("marketing_warehouse_ad_sources") %}

        with
            base as (select * from {{ ref("stg_facebook_ads__creative_history_tmp") }}),

            fields as (

                select
                    {{
                        fivetran_utils.fill_staging_columns(
                            source_columns=adapter.get_columns_in_relation(
                                ref("stg_facebook_ads__creative_history_tmp")
                            ),
                            staging_columns=get_facebook_creative_history_columns(),
                        )
                    }}

                from base
            ),

            fields_xf as (

                select
                    _fivetran_id,
                    id as creative_id,
                    account_id,
                    name as creative_name,
                    page_link,
                    template_page_link,
                    url_tags,
                    asset_feed_spec_link_urls,
                    object_story_link_data_child_attachments,
                    object_story_link_data_caption,
                    object_story_link_data_description,
                    object_story_link_data_link,
                    object_story_link_data_message,
                    template_app_link_spec_ios,
                    template_app_link_spec_ipad,
                    template_app_link_spec_android,
                    template_app_link_spec_iphone,
                    row_number() over (partition by id order by _fivetran_synced desc)
                    = 1 as is_most_recent_record
                from fields

            )

        select *
        from fields_xf

    {% else %} {{ config(enabled=false) }}
    {% endif %}
{% else %} {{ config(enabled=false) }}
{% endif %}
