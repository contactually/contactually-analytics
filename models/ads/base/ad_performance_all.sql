with adwords as (

    select md5(composite_key) as "id", service, date, campaign_id, base_url, utm_source,
           utm_medium, utm_campaign, utm_content, utm_term, impressions, clicks, cost, url
    from {{ ref('adwords_performance_stitch') }}
),

fb_ads as (

    select md5(composite_key), service, insight_date, campaign_id, base_url, utm_source,
           utm_medium, utm_campaign, utm_content, utm_term, impressions, clicks, cost, url
    from {{ ref('facebook_performance_stitch') }}
),

unioned as (

    select * from adwords
    union all
    select * from fb_ads

), normal_channel_mapping as (
    select
        lower(in_medium)   as in_medium,
        lower(in_source)   as in_source,
        lower(in_campaign) as in_campaign,
        lower(in_referer)  as in_referer,
        out_channel,
        out_source,
        out_campaign
    from dbt_dbanin.channel_mapping
),

ad_id_channel_mapping as (
    select
        id,
        coalesce(c1.out_channel, c2.out_channel, c3.out_channel, c4.out_channel, 'Advertising') as channel,
        coalesce(c1.out_source, c2.out_source, c3.out_source, c4.out_source, unioned.utm_source) as source,
        coalesce(c1.out_campaign, c2.out_campaign, c3.out_campaign, c4.out_campaign, unioned.utm_campaign) as campaign

    from unioned

    left outer join normal_channel_mapping as c1
        on lower(utm_medium) = c1.in_medium and lower(utm_source) = c1.in_source and lower(utm_campaign) = c1.in_campaign
    left outer join normal_channel_mapping as c2
        on lower(utm_medium) = c2.in_medium and lower(utm_source) = c2.in_source and c2.in_campaign is null
    left outer join normal_channel_mapping as c3
        on lower(utm_source) = c3.in_source and lower(utm_campaign) = c3.in_campaign and c3.in_medium is null
    left outer join normal_channel_mapping as c4
        on lower(utm_medium) = c4.in_medium and c4.in_source is null and c4.in_campaign is null

    group by 1, 2, 3, 4
),

unique_ad_id_channels as (

  select id, channel, source, campaign from (
    select *,
    rank() over (partition by id order by channel, source, campaign) as rank
    from ad_id_channel_mapping
  )
  where rank = 1

),

with_channel as (
    select
        unioned.id, service, "date", campaign_id, clicks, cost, impressions, url,
        mapped.channel,
        replace(replace(lower(base_url), 'http://', ''), 'https://', '') as base_url,
        replace(replace(replace(lower(utm_medium), '%20', ' '), '+', ' '), '%7c', '|') as utm_medium,
        replace(replace(replace(lower(mapped.source), '%20', ' '), '+', ' '), '%7c', '|') as utm_source,
        replace(replace(replace(lower(mapped.campaign), '%20', ' '), '+', ' '), '%7c', '|') as utm_campaign,
        replace(replace(replace(lower(utm_content), '%20', ' '), '+', ' '), '%7c', '|') as utm_content,
        replace(replace(replace(lower(utm_term), '%20', ' '), '+', ' '), '%7c', '|') as utm_term,

        replace(replace(replace(lower(nullif(utm_medium, '')), '%20', ' '), '+', ' '), '%7c', '|') as original_utm_medium,
        replace(replace(replace(lower(nullif(utm_source, '')), '%20', ' '), '+', ' '), '%7c', '|') as original_utm_source,
        replace(replace(replace(lower(nullif(utm_campaign, '')), '%20', ' '), '+', ' '), '%7c', '|') as original_utm_campaign,
        replace(replace(replace(lower(nullif(utm_term, '')), '%20', ' '), '+', ' '), '%7c', '|') as original_utm_term,
        replace(replace(replace(lower(nullif(utm_content, '')), '%20', ' '), '+', ' '), '%7c', '|') as original_utm_content
    from unioned
        left outer join unique_ad_id_channels as mapped on mapped.id = unioned.id
)

select * from with_channel
where "date" > '2016-07-04'
