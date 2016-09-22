with ads as (

  select * from {{ref('facebook_ads')}}

), creatives as (

  select * from {{ref('facebook_ad_creatives')}}

), insights as (

  select * from {{ref('facebook_ad_insights')}}

), joined as (

  select
    insights.ad_id || '-' || insights.adset_id || '-' || insights.campaign_id || '-' || insights.date_start as composite_key,
    *
  from insights
    inner join ads on insights.ad_id = ads.id
    inner join creatives on ads.creative_id = creatives.id

)

select
    composite_key,
    'fb_ads' as service,
    date_start::date as insight_date,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_content,
    utm_term,
    campaign_id,
    url,
    base_url,
    sum(impressions) as impressions,
    sum(spend) as cost,
    sum(clicks) as clicks
  from joined
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
