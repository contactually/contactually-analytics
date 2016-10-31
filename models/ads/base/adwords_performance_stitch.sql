with rawdata as (

  select
    addestinationurl as url,
    impressions,
    adclicks as clicks,
    adcost as cost,
    date,
    keyword,
    adcontent,
    adwordscampaignid as campaign_id,
    campaign,
    adgroup,
    adcontent || '-' || addestinationurl || '-' || adgroup || '-' || adwordscampaignid || '-' || campaign || '-' || date || '-' || keyword as composite_key
  from _1_all_contactually_property_traffic.adwords89732821_v2
)

select
  md5(composite_key) as id,
  date,
  campaign_id,
  url,
  split_part(url,'?',1) as base_url,
  nullif(split_part(split_part(url,'utm_source=',2), '&', 1), '') as utm_source,
  nullif(split_part(split_part(url,'utm_medium=',2), '&', 1), '') as utm_medium,
  nullif(split_part(split_part(url,'utm_campaign=',2), '&', 1), '') as utm_campaign,
  nullif(split_part(split_part(url,'utm_content=',2), '&', 1), '') as utm_content,
  nullif(split_part(split_part(url,'utm_term=',2), '&', 1), '') as utm_term,
  impressions,
  clicks,
  cost
from rawdata

--next, need to provide a rollup of this view that gets the key metrics that we care about first aggregated by utm params.
