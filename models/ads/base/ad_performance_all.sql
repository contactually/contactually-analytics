with adwords as (

    select md5(composite_key) as "id", service, date, campaign_id, base_url, utm_source,
           utm_medium, utm_campaign, utm_content, utm_term, impressions, clicks, cost
    from {{ ref('adwords_performance_stitch') }}
),

fb_ads as (

    select md5(composite_key), service, insight_date, campaign_id, base_url, utm_source,
           utm_medium, utm_campaign, utm_content, utm_term, impressions, clicks, cost
    from {{ ref('facebook_performance_stitch') }}
)

select * from adwords
union all
select * from fb_ads
