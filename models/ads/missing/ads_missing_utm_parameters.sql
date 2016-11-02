
{{
    config(
        materialized='table',
        sort='date',
        dist='id'
    )
}}


with ads as (

    select * from {{ ref('ad_performance_all') }}

)

select *
from ads
where
    date > '2016-06-01' and
    coalesce(utm_medium, utm_source, utm_campaign, utm_content, utm_term) is null

