select
  id,
  total_amount as totalamt,
  transaction_date as txndate,
  account_id as deposittoaccountref__value,
  null as metadata__createtime,
  null as metadata__lastupdatedtime
from quickbooks.deposit
where not deleted