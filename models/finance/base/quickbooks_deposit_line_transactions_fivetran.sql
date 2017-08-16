select
  deposit_id as source_key,
  line_number as level_id,
  txn_line_id as txnid
from quickbooks.deposit_line_txn