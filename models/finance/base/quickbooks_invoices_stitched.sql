select
    id::int,
    totalamt,
    txndate::date as txn_date,
    duedate::date as due_date,
    balance,
    null as delivery_type,
    null as delivery_time,
    emailstatus as email_status,
    docnumber as doc_number,
    customerref__value::int as customer_id,
    metadata__createtime as created_at,
    metadata__lastupdatedtime as updated_at
from quickbooks.quickbooks_invoices