select
  id,
  name,
  fully_qualified_name as fullyqualifiedname,
  active,
  current_balance as currentbalance,
  parent as parentref__value,
  account_type as accounttype,
  account_sub_type as accountsubtype,
  sub_account as subaccount,
  classification,
  null as metadata__createtime,
  null as metadata__lastupdatedtime
from quickbooks.account
where not deleted