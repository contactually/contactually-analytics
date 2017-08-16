select
  id,
  total_amt as totalamt,
  txn_date as txndate,
  due_date as duedate,
  balance,
  null as deliveryinfo__deliverytype,
  null as deliveryinfo__deliverytime,
  email_status as emailstatus,
  doc_number as docnumber,
  customer as customerref__value,
  null as metadata__createtime,
  null as metadata__lastupdatedtime
from quickbooks_fivetran.invoice