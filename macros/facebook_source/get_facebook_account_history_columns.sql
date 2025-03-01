{% macro get_facebook_account_history_columns() %}

    {% set columns = [
        {"name": "_fivetran_id", "datatype": dbt_utils.type_string()},
        {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()},
        {"name": "account_status", "datatype": dbt_utils.type_string()},
        {"name": "age", "datatype": dbt_utils.type_float()},
        {
            "name": "agency_client_declaration_agency_representing_client",
            "datatype": dbt_utils.type_int(),
        },
        {
            "name": "agency_client_declaration_client_based_in_france",
            "datatype": dbt_utils.type_int(),
        },
        {
            "name": "agency_client_declaration_client_city",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "agency_client_declaration_client_country_code",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "agency_client_declaration_client_email_address",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "agency_client_declaration_client_name",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "agency_client_declaration_client_postal_code",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "agency_client_declaration_client_province",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "agency_client_declaration_client_street",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "agency_client_declaration_client_street_2",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "agency_client_declaration_has_written_mandate_from_advertiser",
            "datatype": dbt_utils.type_int(),
        },
        {
            "name": "agency_client_declaration_is_client_paying_invoices",
            "datatype": dbt_utils.type_int(),
        },
        {"name": "amount_spent", "datatype": dbt_utils.type_int()},
        {"name": "balance", "datatype": dbt_utils.type_int()},
        {"name": "business_city", "datatype": dbt_utils.type_string()},
        {
            "name": "business_country_code",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "business_manager_created_by",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "business_manager_created_time",
            "datatype": dbt_utils.type_timestamp(),
        },
        {
            "name": "business_manager_manager_id",
            "datatype": dbt_utils.type_int(),
        },
        {
            "name": "business_manager_name",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "business_manager_primary_page",
            "datatype": dbt_utils.type_string(),
        },
        {
            "name": "business_manager_timezone_id",
            "datatype": dbt_utils.type_int(),
        },
        {
            "name": "business_manager_update_time",
            "datatype": dbt_utils.type_timestamp(),
        },
        {
            "name": "business_manager_updated_by",
            "datatype": dbt_utils.type_string(),
        },
        {"name": "business_name", "datatype": dbt_utils.type_string()},
        {"name": "business_state", "datatype": dbt_utils.type_string()},
        {"name": "business_street", "datatype": dbt_utils.type_string()},
        {"name": "business_street_2", "datatype": dbt_utils.type_string()},
        {"name": "business_zip", "datatype": dbt_utils.type_string()},
        {"name": "can_create_brand_lift_study", "datatype": "boolean"},
        {"name": "capabilities", "datatype": dbt_utils.type_string()},
        {"name": "created_time", "datatype": dbt_utils.type_timestamp()},
        {"name": "currency", "datatype": dbt_utils.type_string()},
        {"name": "disable_reason", "datatype": dbt_utils.type_string()},
        {"name": "end_advertiser", "datatype": dbt_utils.type_int()},
        {"name": "end_advertiser_name", "datatype": dbt_utils.type_string()},
        {"name": "has_migrated_permissions", "datatype": "boolean"},
        {"name": "id", "datatype": dbt_utils.type_int()},
        {"name": "io_number", "datatype": dbt_utils.type_int()},
        {"name": "is_attribution_spec_system_default", "datatype": "boolean"},
        {"name": "is_direct_deals_enabled", "datatype": "boolean"},
        {"name": "is_notifications_enabled", "datatype": "boolean"},
        {"name": "is_personal", "datatype": dbt_utils.type_int()},
        {"name": "is_prepay_account", "datatype": "boolean"},
        {"name": "is_tax_id_required", "datatype": "boolean"},
        {"name": "media_agency", "datatype": dbt_utils.type_int()},
        {
            "name": "min_campaign_group_spend_cap",
            "datatype": dbt_utils.type_int(),
        },
        {"name": "min_daily_budget", "datatype": dbt_utils.type_int()},
        {"name": "name", "datatype": dbt_utils.type_string()},
        {"name": "next_bill_date", "datatype": dbt_utils.type_timestamp()},
        {"name": "offsite_pixels_tos_accepted", "datatype": "boolean"},
        {"name": "owner", "datatype": dbt_utils.type_int()},
        {"name": "partner", "datatype": dbt_utils.type_int()},
        {
            "name": "salesforce_invoice_group_id",
            "datatype": dbt_utils.type_string(),
        },
        {"name": "spend_cap", "datatype": dbt_utils.type_int()},
        {"name": "tax_id", "datatype": dbt_utils.type_string()},
        {"name": "tax_id_status", "datatype": dbt_utils.type_string()},
        {"name": "tax_id_type", "datatype": dbt_utils.type_string()},
        {"name": "timezone_id", "datatype": dbt_utils.type_int()},
        {"name": "timezone_name", "datatype": dbt_utils.type_string()},
        {
            "name": "timezone_offset_hours_utc",
            "datatype": dbt_utils.type_float(),
        },
    ] %}

    {{ return(columns) }}

{% endmacro %}
