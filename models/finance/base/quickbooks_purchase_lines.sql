select
  index as id,
  amount,
  purchase_id as source_key,
  account_expense_class_id as accountbasedexpenselinedetail__classref__value,
  account_expense_account_id as accountbasedexpenselinedetail__accountref__value
from quickbooks.purchase_line