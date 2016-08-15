with session_ranks as (
  select
  blended_user_id as user_id,
  domain_userid || '-' || domain_sessionidx as session_id,
  rank() over (partition by blended_user_id order by session_start_tstamp) as session_number,
  session_start_tstamp,
  session_end_tstamp,
  event_count,
  user_logged_in,
  channel,
  source,
  mkt_campaign as campaign,
  count(*) over (partition by blended_user_id) as total,
  min(session_start_tstamp) over (partition by blended_user_id) as first_event_date
  from {{ref("sessions_enriched")}} s
  join {{ref("users")}} u on u.id = s.user_id
  where
  (s.session_start_tstamp <= u.created_at or s.dvce_min_tstamp <= u.created_at)
),
first_touch as (
  select user_id, coalesce(channel, 'Direct') as channel, source, campaign
  from session_ranks
  where session_number = 1
),
last_touch as (
  select user_id, coalesce(channel, 'Direct') as channel, source, campaign
  from session_ranks
  where session_number = total and total > 1
),
middle_touches as (
  select distinct user_id,
  listagg(touch, ', ') within group (order by session_number) over (partition by user_id) as touches
  from (
    select user_id, session_number, (coalesce(channel, '(none)') || ' (' || coalesce(source, '(none)') || ') ' || coalesce(campaign, '')) as touch
    from session_ranks
    where session_number > 1 and session_number < total
    and not (channel is null and source is null and campaign is null)
  )
),
users as (
  select distinct user_id, total, first_event_date from session_ranks
),
user_rollup as (
  select  u.user_id,
  u.first_event_date as first_visit,
  u.total as count_sessions,
  ft.channel as first_channel,
  ft.source as first_source,
  ft.campaign as first_campaign,
  mt.touches as middle_touches,
  lt.channel as last_channel,
  lt.source as last_source,
  lt.campaign as last_campaign
  from users u
  left outer join first_touch ft on ft.user_id = u.user_id
  left outer join middle_touches mt on mt.user_id = u.user_id
  left outer join last_touch lt on lt.user_id = u.user_id
)
select * from user_rollup
where first_visit > '2016-07-04'
