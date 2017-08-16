select
  id,
  transaction_date as txndate,
  total_amount as totalamt,
  due_date as duedate,
  balance,
  apaccount as apaccountref__value,
  null as metadata__createtime,
  null as metadata__lastupdatedtime
from quickbooks.bill