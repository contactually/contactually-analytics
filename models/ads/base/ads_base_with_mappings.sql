{{ config(
materialized='table',
sort=['date'],
dist='service')
}}

with smc_mapping as (
    select distinct
      map.in_source,
      map.in_medium,
      map.in_campaign,
      map.out_channel,
      map.out_source,
      map.out_medium,
      map.out_campaign,
      map.smc_key
    from fivetran_uploads.snowplow_mapping map
),
    ads_base_with_smc_key as (
      select
        service,
        date,
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
        cost,
        case when utm_source is not null or utm_medium is not null or utm_campaign is not null
          then lower(nvl( utm_source,'' ) || nvl( utm_medium,'' ) || nvl( utm_campaign,''))
        else null
        end as smc_key
      from {{ ref('ads_base') }}
  )
select
  ads_base.service,
  ads_base.date,
  ads_base.campaign_id,
  ads_base.url,
  ads_base.base_url,
  ads_base.utm_source,
  ads_base.utm_medium,
  ads_base.utm_campaign,
  ads_base.utm_content,
  ads_base.utm_term,
  smc_mapping.out_channel,
  smc_mapping.out_source,
  smc_mapping.out_medium,
  smc_mapping.out_campaign,
  sum(ads_base.impressions) as impressions,
  sum(ads_base.clicks) as clicks,
  sum(ads_base.cost) as cost
from ads_base_with_smc_key ads_base
  left join smc_mapping smc_mapping
    on ads_base.smc_key = lower(smc_mapping.smc_key)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14