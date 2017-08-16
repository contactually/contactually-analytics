select
  id,
  total_amt as totalamt,
  txn_date as txndate,
  unapplied_amt as unappliedamt,
  process_payment as processpayment,
  account as deposittoaccountref__value,
  customer as customerref__value,
  payment_method as paymentmethodref__value,
  null as metadata__createtime,
  null as metadata__lastupdatedtime
from quickbooks_fivetran.payment