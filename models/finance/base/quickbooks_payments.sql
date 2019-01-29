select
  id,
  total_amount as totalamt,
  transaction_date as txndate,
  unapplied_amount as unappliedamt,
  process_payment as processpayment,
  deposit_to_account_id as deposittoaccountref__value,
  customer_id as customerref__value,
  payment_method_id as paymentmethodref__value,
  null as metadata__createtime,
  null as metadata__lastupdatedtime
from quickbooks.payment
where not deleted