with ads as (

  select * from {{ref('facebook_ads')}}

), creatives as (

  select * from {{ref('facebook_ad_creatives')}}

), insights as (

  select * from {{ref('facebook_ad_insights')}}

), joined as (

  select
    *
  from insights
    inner join ads on insights.ad_id = ads.id
    inner join creatives on ads.creative_id = creatives.id

)

select
date_start as date,
campaign_id,
url,
ad_id,
ad_name,
campaign_name,

sum(spend) as cost,
sum(clicks) as clicks

from joined
where
utm_medium is null AND
utm_source is null AND
utm_campaign is null AND
utm_content is null AND
utm_term is null
group by 1, 2, 3, 4, 5, 6
