with ads as (

  select * from {{ref('facebook_ads')}}

), creatives as (

  select * from {{ref('facebook_ad_creatives')}}

), insights as (

  select * from {{ref('facebook_ad_insights')}}

), campaigns as (

  select id as _campaign_id, name as campaign_name from facebook_contactually_ads.facebook_campaigns_26288427

), joined as (

  select
    insights.ad_id || '-' || insights.adset_id || '-' || insights.campaign_id || '-' || insights.date_start as composite_key,
    *
  from insights
    inner join ads on insights.ad_id = ads.id
    inner join creatives on ads.creative_id = creatives.id
    inner join campaigns on campaigns._campaign_id = insights.campaign_id

)

select
    composite_key,
    'fb_ads' as service,
    date_start::date as insight_date,
    ad_id,
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
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
