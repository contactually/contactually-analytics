select
  bill_payment_id as source_key,
  index as level_id,
  amount
from quickbooks_fivetran.bill_payment_line