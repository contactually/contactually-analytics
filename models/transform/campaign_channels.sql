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
        coalesce(c1.channel, c2.channel, c3.channel, c4.channel, '(none)') as channel,
        coalesce(s.mkt_source, c1.source, c2.source, c3.source, c4.source, '(none)') as enriched_source,
        coalesce(s.mkt_medium, c1.medium, c2.medium, c3.medium, c4.medium, '(none)') as enriched_medium,
        s.refr_urlhost_clean
    from sessions as s
        left outer join dbt_dbanin.channel_mapping c1 on (s.mkt_source = c1.source and s.mkt_medium = c1.medium and s.refr_urlhost_clean = c1.source)
        left outer join dbt_dbanin.channel_mapping c2 on (s.mkt_source = c2.source and s.mkt_medium = c2.medium)
        left outer join dbt_dbanin.channel_mapping c3 on (s.refr_urlhost_clean = c3.source)
        left outer join dbt_dbanin.channel_mapping c4 on (s.refr_urlhost_clean = c4.referer)
    where not (s.mkt_source is null and s.mkt_medium is null and s.mkt_campaign is null and s.refr_urlhost is null)
    group by 1, 2, 3, 4, 5
), unique_session_channels as (
    select * from (
        select *, row_number() over (partition by session_id) as row_num
        from session_channels
    )
    where row_num = 1
)
select session_id, channel, enriched_source, enriched_medium from unique_session_channels
