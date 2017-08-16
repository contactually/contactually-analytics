select
  bill_payment_id as source_key,
  line_index as level_id,
  txn_id as txnid
from quickbooks_fivetran.bill_payment_line_transaction