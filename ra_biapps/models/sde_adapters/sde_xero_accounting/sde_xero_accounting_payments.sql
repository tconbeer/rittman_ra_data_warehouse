{% if not enable_xero_accounting %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH
  payments AS (
          SELECT
            *
          FROM (
            SELECT
              *,
              MAX(_sdc_batched_at) OVER (PARTITION BY paymentid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at
            FROM
              {{ source('xero_accounting', 'payments') }})
          WHERE
            latest_sdc_batched_at = _sdc_batched_at
          )
  SELECT
   'xero_accounting'       source,
    paymentid as payment_id,
    account.accountid as payment_account_id,
    account.code as payment_code,
    invoice.contact.contactid as payment_contact_id,
    invoice.isdiscounted as payment_is_discounted,
    invoice.currencycode as payment_currency_code,
    invoice.invoicenumber as payment_invoice_number,
    invoice.invoiceid as payment_invoice_id,
    invoice.type as payment_invoice_type,
    status as payment_status,
    paymenttype as payment_type,
    reference as payment_reference,
    amount as payment_amount,
    date as payment_date,
    isreconciled as payment_is_reconciled,
    bankamount as payment_bank_amount,
    currencyrate as payment_currency_rate
  FROM payments
