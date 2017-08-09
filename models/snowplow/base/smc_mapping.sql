select distinct
  lower(map.smc_key) as smc_key,
  lower(map.in_source) as in_source,
  lower(map.in_medium) as in_medium,
  lower(map.in_campaign) as in_campaign,
  lower(map.out_channel) as out_channel,
  lower(map.out_source) as out_source,
  lower(map.out_medium) as out_medium,
  lower(map.out_campaign) as out_campaign,
  lower(map.out_referring_domain) as out_referring_domain,
  lower(map.out_intent) as out_intent,
  lower(map.out_clicktype) as out_clicktype
from fivetran_uploads.snowplow_mapping map