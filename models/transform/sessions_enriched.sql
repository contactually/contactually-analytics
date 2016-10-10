with sessions as (
    select * from {{ref('sessions_joined')}}
),
normalized_sessions as (
    select
    sessions.*,
    case when refr_medium = 'unknown' then
        lower(mkt_medium)
    else
        lower(coalesce(mkt_medium, refr_medium))
    end as medium,
    lower(coalesce(mkt_source, refr_source)) as source,
    lower(mkt_campaign) as campaign,
    lower(replace(refr_urlhost, 'www.', '')) as refr_urlhost_clean
    from sessions
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
session_channels as (
    select
        s.session_id,
        case when
            coalesce(refr_urlhost, mkt_medium, mkt_source, mkt_campaign, refr_medium, refr_source) is null then 'direct'
        else
            lower(coalesce(c1.out_channel, c2.out_channel, c3.out_channel, c4.out_channel, c5.out_channel, 'referral'))
        end as channel,
        coalesce(c1.out_source, c2.out_source, c3.out_source, c4.out_source, c5.out_source, s.source, s.refr_urlhost_clean) as source,
        coalesce(c1.out_campaign, c2.out_campaign, c3.out_campaign, c4.out_campaign, c5.out_source, s.campaign) as campaign,
        s.refr_urlhost_clean as referer
    from normalized_sessions as s
        left outer join normal_channel_mapping as c1
            on s.medium = c1.in_medium and s.source = c1.in_source and s.campaign = c1.in_campaign
        left outer join normal_channel_mapping as c2
            on s.medium = c2.in_medium and s.source = c2.in_source and c2.in_campaign is null
        left outer join normal_channel_mapping as c3
            on s.source = c3.in_source and s.campaign = c3.in_campaign and c3.in_medium is null
        left outer join normal_channel_mapping as c4
            on s.medium = c4.in_medium and c4.in_source is null and c4.in_campaign is null
        left outer join normal_channel_mapping as c5
            on s.refr_urlhost_clean ilike c5.in_referer
    group by 1, 2, 3, 4, 5
),
unique_session_channels as (
  select session_id, channel, source, campaign, referer from (
    select *,
    rank() over (partition by session_id order by channel, source, campaign, referer) as rank
    from session_channels
  )
  where rank = 1
)
select s.*,
coalesce(s.refr_urlhost, '') || coalesce(s.refr_urlpath, '') as referrer_url,
usc.channel, usc.source, usc.campaign,
mkt_source as original_mkt_source, mkt_medium as original_mkt_medium,
mkt_campaign as original_mkt_campaign, mkt_term as original_mkt_term,
mkt_content as original_mkt_content
from sessions s
left outer join unique_session_channels usc on s.session_id = usc.session_id
