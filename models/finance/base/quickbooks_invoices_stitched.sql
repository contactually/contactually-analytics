select
    id::int,
    totalamt,
    txndate::date as txndate,
    duedate::date as duedate,
    balance,
    null as deliveryinfo__deliverytype,
    null as deliveryinfo__deliverytime,
    emailstatus as emailstatus,
    docnumber as docnumber,
    customerref__value::int as customerref__value,
    metadata__createtime as metadata__createtime,
    metadata__lastupdatedtime as metadata__lastupdatedtime
from quickbooks.quickbooks_invoices