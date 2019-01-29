select
  id,
  name,
  unit_price as unitprice,
  type,
  taxable,
  income_account_id as incomeaccountref__value,
  null as metadata__createtime,
  null as metadata__lastupdatedtime
from quickbooks.item
where not deleted