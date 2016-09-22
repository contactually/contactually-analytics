with ads as (
    select * from {{ ref('ad_performance_all') }}
)
select
    md5(
        coalesce(service, '') ||
        '-'  ||
        coalesce(campaign_id, '') ||
        '-'  ||
        coalesce(channel, '') ||
        '-'  ||
        coalesce(base_url, '') ||
        '-'  ||
        coalesce(utm_medium, '') ||
        '-'  ||
        coalesce(utm_source, '') ||
        '-'  ||
        coalesce(utm_campaign, '') ||
        '-'  ||
        coalesce(utm_content, '') ||
        '-'  ||
        coalesce(utm_term, '')
    ) as id,
    service,
    campaign_id,
    channel,
    base_url,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_content,
    utm_term,
    original_utm_medium,
    original_utm_source,
    original_utm_campaign,
    original_utm_content,
    original_utm_term,
    min("date") as min_date,
    max("date") as max_date,
    sum(clicks) as clicks,
    sum(cost) as cost,
    sum(impressions) as impressions
from ads
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
