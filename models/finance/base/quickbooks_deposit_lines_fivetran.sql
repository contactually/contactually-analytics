select
  deposit_id as source_key,
  line_number as level_id,
  amount,
  deposit_lineclass as depositlinedetail__classref__value,
  deposit_lineaccount as depositlinedetail__accountref__value
from quickbooks.deposit_line