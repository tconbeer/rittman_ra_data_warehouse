{% macro get_facebook_ad_set_history_columns() %}

    {% set columns = [
        {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()},
        {"name": "account_id", "datatype": dbt_utils.type_int()},
        {"name": "adset_source_id", "datatype": dbt_utils.type_int()},
        {"name": "bid_amount", "datatype": dbt_utils.type_int()},
        {"name": "bid_info_actions", "datatype": dbt_utils.type_int()},
        {"name": "bid_strategy", "datatype": dbt_utils.type_string()},
        {"name": "billing_event", "datatype": dbt_utils.type_string()},
        {"name": "budget_remaining", "datatype": dbt_utils.type_int()},
        {"name": "campaign_id", "datatype": dbt_utils.type_int()},
        {"name": "configured_status", "datatype": dbt_utils.type_string()},
        {"name": "created_time", "datatype": dbt_utils.type_timestamp()},
        {"name": "daily_budget", "datatype": dbt_utils.type_int()},
        {"name": "destination_type", "datatype": dbt_utils.type_string()},
        {"name": "effective_status", "datatype": dbt_utils.type_string()},
        {"name": "end_time", "datatype": dbt_utils.type_timestamp()},
        {"name": "id", "datatype": dbt_utils.type_int()},
        {"name": "instagram_actor_id", "datatype": dbt_utils.type_int()},
        {"name": "lifetime_budget", "datatype": dbt_utils.type_int()},
        {"name": "lifetime_imps", "datatype": dbt_utils.type_int()},
        {"name": "name", "datatype": dbt_utils.type_string()},
        {"name": "optimization_goal", "datatype": dbt_utils.type_string()},
        {
            "name": "promoted_object_application_id",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "promoted_object_custom_event_type",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "promoted_object_event_id",
            "datatype": dbt_utils.type_int(),
        },
        {
            "name": "promoted_object_object_store_url",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "promoted_object_offer_id",
            "datatype": dbt_utils.type_int(),
        },
        {"name": "promoted_object_page_id", "datatype": dbt_utils.type_int()},
        {
            "name": "promoted_object_pixel_id",
            "datatype": dbt_utils.type_int(),
        },
        {
            "name": "promoted_object_place_page_set_id",
            "datatype": dbt_utils.type_int(),
        },
        {
            "name": "promoted_object_product_catalog_id",
            "datatype": dbt_utils.type_int(),
        },
        {
            "name": "promoted_object_product_set_id",
            "datatype": dbt_utils.type_int(),
        },
        {"name": "recurring_budget_semantics", "datatype": "boolean"},
        {"name": "rf_prediction_id", "datatype": dbt_utils.type_string()},
        {"name": "start_time", "datatype": dbt_utils.type_timestamp()},
        {"name": "status", "datatype": dbt_utils.type_string()},
        {"name": "targeting_age_max", "datatype": dbt_utils.type_int()},
        {"name": "targeting_age_min", "datatype": dbt_utils.type_int()},
        {
            "name": "targeting_app_install_state",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_audience_network_positions",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_college_years",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_connections",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_device_platforms",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_education_majors",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_education_schools",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_education_statuses",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_effective_audience_network_positions",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_excluded_connections",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_excluded_publisher_categories",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_excluded_publisher_list_ids",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_excluded_user_device",
            "datatype": dbt_utils.type_string(),
        },
        {"name": "targeting_exclusions", "datatype": dbt_utils.type_string()},
        {
            "name": "targeting_facebook_positions",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_flexible_spec",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_friends_of_connections",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_geo_locations_countries",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_geo_locations_location_types",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_instagram_positions",
            "datatype": dbt_utils.type_string(),
        },
        {"name": "targeting_locales", "datatype": dbt_utils.type_string()},
        {
            "name": "targeting_publisher_platforms",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_user_adclusters",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_user_device",
            "datatype": dbt_utils.type_string(),
        },
        {"name": "targeting_user_os", "datatype": dbt_utils.type_string()},
        {
            "name": "targeting_wireless_carrier",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_work_employers",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "targeting_work_positions",
            "datatype": dbt_utils.type_string(),
        },
        {"name": "updated_time", "datatype": dbt_utils.type_timestamp()},
        {"name": "use_new_app_click", "datatype": "boolean"},
    ] %}

    {{ return(columns) }}

{% endmacro %}
