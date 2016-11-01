
{{ config(
    materialized='table',
    sort=['session_id', 'ad_id'],
    dist='session_id'
  )
}}


-- coalesce null values to empty strings so that our join on utm params
-- can be a simple equality. Using (ad.utm_* = session.utm_* OR ad.utm_* is null) 
-- results in an ugly nested-loop join and kills performance

with sessions as (

    select session_id,
        session_start_tstamp::date as date,
        coalesce(cleaned_source, '')   as cleaned_source,
        coalesce(cleaned_medium, '')   as cleaned_medium,
        coalesce(cleaned_campaign, '') as cleaned_campaign,
        coalesce(cleaned_term, '')     as cleaned_term,
        coalesce(cleaned_content, '')  as cleaned_content

    from {{ ref('sp_sessions_with_attribution') }}

),

ads as (

    select id,
        date::date as date,
        coalesce(utm_source, '')   as utm_source,
        coalesce(utm_medium, '')   as utm_medium,
        coalesce(utm_campaign, '') as utm_campaign,
        coalesce(utm_term, '')     as utm_term,
        coalesce(utm_content, '')  as utm_content

    from {{ ref('ad_performance_all') }}
    where
        utm_source is not null and
        utm_medium is not null and
        utm_campaign is not null and
        utm_term is not null and
        utm_content is not null

),


with_ad_id as (
    select
        ads.id as ad_id,
        s.session_id,
        row_number() over (partition by s.session_id order by ads.id) as row_number
    from sessions as s
    inner join ads on
        ads.utm_source   = s.cleaned_source   and
        ads.utm_medium   = s.cleaned_medium   and
        ads.utm_campaign = s.cleaned_campaign and
        ads.utm_term     = s.cleaned_term     and
        ads.utm_content  = s.cleaned_content  and
        ads.date         = s.date
)


select ad_id, session_id
from with_ad_id
where row_number = 1
