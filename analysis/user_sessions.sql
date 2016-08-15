with session_ranks as (
  select
  blended_user_id,
  user_id,
  domain_userid || '-' || domain_sessionidx as session_id,
  rank() over (partition by blended_user_id order by session_start_tstamp) as session_number,
  session_start_tstamp,
  session_end_tstamp,
  event_count,
  user_logged_in,
  channel,
  source,
  medium,
  mkt_campaign as campaign,
  count(*) over (partition by blended_user_id) as total
  from {{ref('sessions_enriched')}} s
  join {{ref('users')}}on users.id = s.user_id
  where
  --and s.session_start_tstamp > '2016-07-07'
  --and users.created_at > '2016-07-07'
  s.session_start_tstamp::date <= users.created_at::date
)
select session_ranks.* from session_ranks
order by blended_user_id desc, session_number;
