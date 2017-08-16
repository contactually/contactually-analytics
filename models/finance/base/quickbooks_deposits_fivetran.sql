select
  id,
  total_amount as totalamt,
  txn_date as txndate,
  account as deposittoaccountref__value,
  null as metadata__createtime,
  null as metadata__lastupdatedtime
from quickbooks.deposit
where not deleted