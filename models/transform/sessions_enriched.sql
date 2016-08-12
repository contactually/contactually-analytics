with sessions as (
    select * from {{ref('sessions_joined')}}
),
normalized_sessions as (
    select
    sessions.*,
    replace(lower(trim(mkt_source)), '+', ' ') as norm_mkt_source,
    replace(lower(trim(mkt_medium)), '+', ' ') as norm_mkt_medium,
    replace(lower(trim(mkt_campaign)), '+', ' ') as norm_mkt_campaign,
    replace(refr_urlhost, 'www.', '') as refr_urlhost_clean
    from sessions
), session_channels as (
    select
        s.session_id,
        coalesce(c1.channel, c2.channel, c3.channel, c4.channel, '(none)') as channel,
        coalesce(s.norm_mkt_source, c1.source, c2.source, c3.source, c4.source, '(none)') as enriched_source,
        coalesce(s.norm_mkt_medium, c1.medium, c2.medium, c3.medium, c4.medium, '(none)') as enriched_medium,
        s.refr_urlhost_clean
    from normalized_sessions as s
        left outer join dbt_dbanin.channel_mapping c1 on (s.norm_mkt_source = c1.source and s.norm_mkt_medium = c1.medium and s.refr_urlhost_clean = c1.source)
        left outer join dbt_dbanin.channel_mapping c2 on (s.norm_mkt_source = c2.source and s.norm_mkt_medium = c2.medium)
        left outer join dbt_dbanin.channel_mapping c3 on (s.refr_urlhost_clean = c3.source)
        left outer join dbt_dbanin.channel_mapping c4 on (s.refr_urlhost_clean = c4.referer)
    where not (s.norm_mkt_source is null and s.norm_mkt_medium is null and s.norm_mkt_campaign is null and s.refr_urlhost_clean is null)
    group by 1, 2, 3, 4, 5
), unique_session_channels as (
    select * from (
        select *, row_number() over (partition by session_id order by channel, enriched_medium, enriched_source) as row_num
        from session_channels
    )
    where row_num = 1
)
select sessions.*, initcap(channel) channel, lower(enriched_source) enriched_source, lower(enriched_medium) enriched_medium
from sessions
left outer join unique_session_channels on sessions.session_id = unique_session_channels.session_id
