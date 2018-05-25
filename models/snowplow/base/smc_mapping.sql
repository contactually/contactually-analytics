select distinct
  lower(map.smc_key) as smc_key,
  max(lower(map.in_source)) as in_source,
  max(lower(map.in_medium)) as in_medium,
  max(lower(map.in_campaign)) as in_campaign,
  max(lower(map.in_referer)) as in_referer,
  max(lower(map.out_channel)) as out_channel,
  max(lower(map.out_source)) as out_source,
  max(lower(map.out_medium)) as out_medium,
  max(lower(map.out_campaign)) as out_campaign,
  max(lower(map.out_referring_domain)) as out_referring_domain,
  max(lower(map.out_category)) as out_category,
  max(lower(map.clean_landing_page)) as clean_landing_page
from fivetran_uploads.snowplow_mapping map
group by 1