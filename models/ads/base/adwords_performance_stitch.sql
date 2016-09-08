with rawdata as (

  select
    'adwords'::text as service,
    addestinationurl as url,
    impressions,
    adclicks as clicks,
    adcost as cost,
    date,
    keyword,
    adcontent,
    adwordscampaignid as campaign_id,
    adgroup as ad_group_id,  -- TODO : this is a string, not an id
    null click_type -- TODO : not provided?
  from _1_all_contactually_property_traffic.adwords89732821_v2
)

select
  service || '-' || date::text || '-' || campaign_id::text || '-' || ad_group_id || '-' || adcontent || '-' ||  url || '-' || keyword as composite_key,
  service,
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
