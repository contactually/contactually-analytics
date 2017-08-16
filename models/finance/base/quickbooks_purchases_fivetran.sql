select
  id,
  payment_type as paymenttype,
  total_amt as totalamt,
  transaction_date as txndate,
  credit,
  account as accountref__value,
  entityvendor_id as entityref__value,
  null as entityref__name,
  entitytype as entityref__type,
  null as metadata__createtime,
  null as metadata__lastupdatedtime
from quickbooks_fivetran.purchase