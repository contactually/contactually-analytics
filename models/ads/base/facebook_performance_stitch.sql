
with insights as (

    select * from {{ ref('fb_ad_insights_xf') }}

)


select 
  id,
  date_day as date,
  campaign_id,
  url,
  base_url,
  utm_source,
  utm_medium,
  utm_campaign,
  utm_content,
  utm_term,
  impressions,
  clicks,
  spend as cost

from insights
