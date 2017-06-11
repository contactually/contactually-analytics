

{{ config(
    materialized='table',
    sort=['id'],
    dist='service',
    enabled='False'
  )
}}

with base as (

    select *,
        row_number() over (partition by id order by channel_mapping_id) as dedupe

    from {{ ref('ad_performance_all_base') }}


)

select
    md5(
        service
        || '-' || date
        || '-' || coalesce(campaign_id, '')
        || '-' || coalesce(url, '')
        || '-' || coalesce(base_url, '')
        || '-' || coalesce(utm_medium, '')
        || '-' || coalesce(mapped_source, '')
        || '-' || coalesce(mapped_campaign, '')
        || '-' || coalesce(utm_content, '')
        || '-' || coalesce(utm_term, '')
    ) as id,
    service,
    date,
    campaign_id,
    url,
    base_url,
    utm_medium,
    mapped_source as utm_source,
    mapped_campaign as utm_campaign,
    utm_content,
    utm_term,

    -- for joining to sessions
    utm_source as og_utm_source,
    utm_campaign as og_utm_campaign,

    sum(impressions) as impressions,
    sum(clicks)      as clicks,
    sum(cost)        as cost
from base
where dedupe = 1
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
