select
  line_num as id,
  amount,
  description,
  invoice_id as source_key,
  sales_item_lineclass as salesitemlinedetail__classref__value,
  sales_item_lineitem as salesitemlinedetail__itemref__value,
  detail_type as detailtype
from quickbooks_fivetran.invoice_line