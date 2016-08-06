select *
from snowplow.event
where domain_userid is not null
  and (refr_urlhost != 'contactuallyh.staging.wpengine.com' or refr_urlhost is null)
  and (user_ipaddress != '209.66.80.204' or user_ipaddress is null)
