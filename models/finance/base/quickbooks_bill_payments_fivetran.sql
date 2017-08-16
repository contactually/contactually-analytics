select
  id,
  total_amt as totalamt,
  transaction_date as txndate,
  vendor as vendorref__value,
  credit_card as creditcardpayment__ccaccountref__value,
  bank_account as checkpayment__bankaccountref__value,
  null as metadata__createtime,
  null as metadata__lastupdatedtime
from quickbooks.bill_payment
where not deleted