select
  id,
  transaction_date as txndate,
  adjustment,
  null as metadata__createtime,
  null as metadata__lastupdatedtime
from quickbooks.journal_entry
where not deleted