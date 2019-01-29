select
  bill_payment_id as source_key,
  index as level_id,
  bill_id as txnid
from quickbooks.bill_linked_txn