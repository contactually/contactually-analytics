select
  id,
  amount,
  purchase_id as source_key,
  account_expenseclass as accountbasedexpenselinedetail__classref__value,
  account_expenseaccount as accountbasedexpenselinedetail__accountref__value
from quickbooks_fivetran.purchase_line