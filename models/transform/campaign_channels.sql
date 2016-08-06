with sessions as (
	select
	replace(lower(trim(mkt_source)), '+', ' ') as mkt_source,
	replace(lower(trim(mkt_medium)), '+', ' ') as mkt_medium,
	replace(lower(trim(mkt_campaign)), '+', ' ') as mkt_campaign,
	refr_urlhost,
    replace(refr_urlhost, 'www.', '') as refr_urlhost_clean,
	domain_userid || '-' || domain_sessionidx as session_id
	from {{ ref('sessions') }}
), session_channels as (
    select
        s.session_id,
        case when s.refr_urlhost_clean = 'contactually.com' then 'Internal'
        else
            coalesce(c1.channel, c2.channel, c3.channel, c4.channel, c5.channel, 'unknown')
        end as channel,
        coalesce(s.mkt_source, c1.source, c2.source, c3.source, c5.source, c4.source) as enriched_source,
        coalesce(s.mkt_medium, c1.medium, c2.medium, c3.medium, c5.medium, c4.medium) as enriched_medium
    from sessions as s
    left outer join {{ ref('campaign_mapping') }} c1 on (s.mkt_source = c1.source AND s.mkt_medium = c1.medium AND s.mkt_campaign = c1.campaign)
    left outer join {{ ref('campaign_mapping') }} c2 on (s.mkt_source = c2.source AND s.mkt_medium = c2.medium)
    left outer join {{ ref('campaign_mapping') }} c3 on (s.mkt_source = c3.source)
    left outer join {{ ref('campaign_mapping') }} c4 on (s.refr_urlhost = c4.source or s.refr_urlhost_clean = c4.source)
    left outer join {{ ref('campaign_referers') }} c5 on (s.refr_urlhost = c5.domain)
    where
        (s.mkt_source is not null and s.mkt_medium is not null and s.mkt_campaign is not null and s.refr_urlhost is not null) or
        (s.mkt_source is not null and s.mkt_medium is not null) or
        (s.mkt_source is not null) or
        (s.refr_urlhost is not null)
    group by 1, 2, 3, 4
), unique_session_channels as (
    select * from (
        select *, row_number() over (partition by session_id) as row_num
        from session_channels
    )
    where row_num = 1
)
select session_id, channel, enriched_source, enriched_medium from unique_session_channels
