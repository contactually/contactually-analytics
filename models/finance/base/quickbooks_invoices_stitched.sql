select
    id::int,
    totalamt,
    txndate::date as txndate,
    duedate::date as duedate,
    balance,
    null as delivery_type,
    null as delivery_time,
    emailstatus as emailstatus,
    docnumber as docnumber,
    customerref__value::int as customerref__value,
    metadata__createtime as metadata__createtime,
    metadata__lastupdatedtime as metadata__lastupdatedtime
from quickbooks.quickbooks_invoices