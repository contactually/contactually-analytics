select
  'Facebook Ads' as ad_source,
  date_start as date,
  campaign_name,
  sum(clicks) as clicks,
  sum(impressions) as impressions,
  sum(spend) as cost
from facebook_contactually_ads.ads_insights ads
group by 1,2,3