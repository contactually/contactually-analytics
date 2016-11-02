
with ads as (

    select * from {{ ref('ad_performance_all') }}

)

select *
from ads
where
    coalesce(utm_medium, utm_source, utm_campaign, utm_content, utm_term) is null
