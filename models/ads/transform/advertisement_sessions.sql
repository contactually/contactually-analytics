
{{ config(
    materialized='table',
    sort=['session_id', 'ad_id'],
    dist='session_id'
  )
}}

with sessions as (

    select * from {{ ref('sessions_with_attribution') }}

),

ads as (

    select * from {{ ref('ad_performance_all') }}

),


with_ad_id as (
    select
        ads.id as ad_id,
        s.session_id,
        row_number() over (partition by s.session_id) as row_number
    from sessions as s
    inner join ads on
        --(s.landing_page = ads.base_url OR ads_rollup.base_url is null) AND
        (ads.utm_source   ilike s.cleaned_source   or ads.utm_source   is null) AND
        (ads.utm_medium   ilike s.cleaned_medium   or ads.utm_medium   is null) AND
        (ads.utm_campaign ilike s.cleaned_campaign or ads.utm_campaign is null) AND
        (ads.utm_term     ilike s.cleaned_term     or ads.utm_term     is null) AND
        (ads.utm_content  ilike s.cleaned_content  or ads.utm_content  is null) AND
        NOT (
          ads.utm_medium   is null AND
          ads.utm_source   is null AND
          ads.utm_campaign is null AND
          ads.utm_term     is null AND
          ads.utm_content  is null
        )
)


select ad_id, session_id from with_ad_id
where row_number = 1
