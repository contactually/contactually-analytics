select
  line_id as id,
  amount,
  bill_id as source_key,
  account_expenseclass as accountbasedexpenselinedetail__classref__value,
  account_expenseaccount as accountbasedexpenselinedetail__accountref__value
from quickbooks.bill_line