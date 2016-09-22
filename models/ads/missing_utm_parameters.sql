select
    "date",
    case when utm_medium is null then TRUE else FALSE end as medium_is_null,
    case when utm_source is null then TRUE else FALSE end as source_is_null,
    case when utm_campaign is null then TRUE else FALSE end as campaign_is_null,
    case when utm_term is null then TRUE else FALSE end as term_is_null,
    case when utm_content is null then TRUE else FALSE end as content_is_null
from {{ ref('ad_performance_all') }}
