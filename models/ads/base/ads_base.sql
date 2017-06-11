{{ config(
materialized='table',
sort=['date'],
dist='service'
enabled=FALSE
)
}}

with versions as (
    select
      *,
      count( * ) over (partition by id rows between unbounded preceding and unbounded following ) as num_versions,
      count( * ) over ( partition by id order by updated_time rows between unbounded preceding and current row ) as version_number
    from facebook_contactually_ads.facebook_ads_26288427 ads
),
    ads as (
      select
        case
        when version_number = 1
          then created_time
        when version_number > 1
          then updated_time
        end as effective_from,
        case
        when version_number = num_versions
          then null
        else lead( updated_time )
        over (
          partition by id
          order by updated_time )
        end as effective_to,
        versions.id,
        versions.creative__id as creative_id
      from versions
  ),
    creatives_base as (
      select
        id,
        lower( nullif( url_tags,'' ) ) as url_tags,
        lower( coalesce(
                   nullif( object_story_spec__link_data__call_to_action__value__link,'' ),
                   nullif( object_story_spec__video_data__call_to_action__value__link,'' ),
                   nullif( null,'' ),
                   nullif( object_story_spec__link_data__link,'' ),
                   nullif( null,'' )
               ) ) as url
      from
        facebook_contactually_ads.facebook_adcreative_26288427 creatives
  ),
    creative_splits as (
      select
        id,
        url,
        split_part( url,'?',1 ) as base_url,
        coalesce( url_tags,split_part( url,'?',2 ) ) as url_tags
      from creatives_base
  ),
    creatives as (
      select
        *,
        split_part( split_part( url_tags,'utm_source=',2 ),'&',1 ) as utm_source,
        split_part( split_part( url_tags,'utm_medium=',2 ),'&',1 ) as utm_medium,
        split_part( split_part( url_tags,'utm_campaign=',2 ),'&',1 ) as utm_campaign,
        split_part( split_part( url_tags,'utm_content=',2 ),'&',1 ) as utm_content,
        split_part( split_part( url_tags,'utm_term=',2 ),'&',1 ) as utm_term
      from creative_splits
  ),
    facebooks_ads as (
      select
        insights.date_start :: DATE as date_day,
        insights.campaign_id,
        creatives.url,
        creatives.base_url,
        creatives.utm_medium,
        creatives.utm_source,
        creatives.utm_campaign,
        creatives.utm_content,
        creatives.utm_term,
        insights.impressions,
        insights.clicks,
        insights.spend as cost
      from facebook_contactually_ads.facebook_ads_insights_26288427 insights
        left join ads
          on insights.ad_id = ads.id
             and insights.date_start :: DATE >= date_trunc( 'day',ads.effective_from ) :: DATE
             and (insights.date_start :: DATE < date_trunc( 'day',ads.effective_to ) :: DATE or ads.effective_to is null)
        left join creatives
          on ads.creative_id = creatives.id
  ),
    unioned as
  (select
     'adwords' as service,
     adwords.date,
     adwords.adwordscampaignid as campaign_id,
     replace( replace( lower( nullif( trim( adwords.addestinationurl ),'' ) ),'http://','' ),'https://','' ) as url,
     split_part( adwords.addestinationurl,'?',1 ) as base_url,
     nullif( split_part( split_part( adwords.addestinationurl,'utm_source=',2 ),'&',1 ),'' ) as utm_source,
     nullif( split_part( split_part( adwords.addestinationurl,'utm_medium=',2 ),'&',1 ),'' ) as utm_medium,
     nullif( split_part( split_part( adwords.addestinationurl,'utm_campaign=',2 ),'&',1 ),'' ) as utm_campaign,
     nullif( split_part( split_part( adwords.addestinationurl,'utm_content=',2 ),'&',1 ),'' ) as utm_content,
     nullif( split_part( split_part( adwords.addestinationurl,'utm_term=',2 ),'&',1 ),'' ) as utm_term,
     adwords.impressions,
     adwords.adclicks as clicks,
     adwords.adcost as cost
   from _1_all_contactually_property_traffic.adwords89732821_v2 adwords
   where adwords.date > '2016-07-04'
   union all
   select
     'fb-ads' as service,
     date_day as date,
     campaign_id,
     url,
     replace( replace( lower( nullif( trim( facebooks_ads.base_url ),'' ) ),'http://','' ),'https://','' ) as base_url,
     utm_source,
     utm_medium,
     utm_campaign,
     utm_content,
     utm_term,
     impressions,
     clicks,
     cost
   from facebooks_ads
   where date_day > '2016-07-04'
  )
select
  service,
  date,
  campaign_id,
  url,
  base_url,
  replace(replace(replace(lower(nullif(trim(utm_source), '')), '%20', ' '), '+', ' '), '%7c', '|') as utm_source,
  replace(replace(replace(lower(nullif(trim(utm_medium), '')), '%20', ' '), '+', ' '), '%7c', '|') as utm_medium,
  replace(replace(replace(lower(nullif(trim(utm_campaign), '')), '%20', ' '), '+', ' '), '%7c', '|') as utm_campaign,
  replace(replace(replace(lower(nullif(trim(utm_content), '')), '%20', ' '), '+', ' '), '%7c', '|') as utm_content,
  replace(replace(replace(lower(nullif(trim(utm_term), '')), '%20', ' '), '+', ' '), '%7c', '|') as utm_term,
  impressions,
  clicks,
  cost
from unioned