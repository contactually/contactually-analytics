select
  deposit_id as source_key,
  index as level_id,
  amount,
  deposit_class_id as depositlinedetail__classref__value,
  deposit_account_id as depositlinedetail__accountref__value
from quickbooks.deposit_line