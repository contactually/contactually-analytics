with adwords as (

    select id, service, date, campaign_id, ad_group_id, click_type, base_url, utm_source,
           utm_medium, utm_campaign, utm_content, utm_term, impressions, clicks, cost
    from {{ ref('adwords_performance_stitch') }}
)

select * from adwords
-- union all ...

