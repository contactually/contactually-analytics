select
  id,
  name,
  fully_qualified_name as fullyqualifiedname,
  active,
  parent as parentref__value,
  sub_class as subclass,
  null as sparse,
  null as domain,
  null as metadata__createtime,
  null as metadata__lastupdatedtime
from quickbooks.class
where not deleted