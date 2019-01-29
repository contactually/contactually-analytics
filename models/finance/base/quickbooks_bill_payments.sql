select
  id,
  total_amount as totalamt,
  transaction_date as txndate,
  vendor_id as vendorref__value,
  credit_card_account_id as creditcardpayment__ccaccountref__value,
  check_bank_account_id as checkpayment__bankaccountref__value,
  null as metadata__createtime,
  null as metadata__lastupdatedtime
from quickbooks.bill_payment
where not deleted