select
    "date",
    campaign_id,
    url,
    --adgroup,
    campaign,
    adcontent,
    sum(clicks) as clicks,
    sum(cost) as cost
from {{ ref('adwords_performance_stitch') }}
where
    utm_medium is null and
    utm_source is null and
    utm_campaign is null and
    utm_term is null and
    utm_content is null
group by 1, 2, 3, 4, 5
