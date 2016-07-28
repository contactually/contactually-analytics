select *
from snowplow.event
where domain_userid is not null
  and (refr_urlhost != 'contactuallyh.staging.wpengine.com' or refr_urlhost is null)
  and (se_category != 'identify' or se_category is null)
