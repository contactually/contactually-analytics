select
  id,
  payment_type as paymenttype,
  total_amount as totalamt,
  transaction_date as txndate,
  credit,
  account_id as accountref__value,
  vendor_id as entityref__value,
  null as entityref__name,
  null as entityref__type,
  null as metadata__createtime,
  null as metadata__lastupdatedtime
from quickbooks.purchase
where not deleted