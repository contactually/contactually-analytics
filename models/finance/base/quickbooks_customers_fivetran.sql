select
  id,
  company_name as companyname,
  active,
  balance,
  null as metadata__createtime,
  null as metadata__lastupdatedtime
from quickbooks_fivetran.customer