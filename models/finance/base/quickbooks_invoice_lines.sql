select
  index as id,
  amount,
  description,
  invoice_id as source_key,
  sales_item_class_id as salesitemlinedetail__classref__value,
  sales_item_item_id as salesitemlinedetail__itemref__value,
  null as detailtype
from quickbooks.invoice_line