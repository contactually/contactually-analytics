with session_ranks as (
  select
  blended_user_id as user_id,
  domain_userid || '-' || domain_sessionidx as session_id,
  rank() over (partition by blended_user_id order by session_start_tstamp) as session_number,
  session_start_tstamp,
  session_end_tstamp,
  event_count,
  user_logged_in,
  coalesce(channel, 'Direct') as channel,
  source,
  mkt_campaign as campaign,
  count(*) over (partition by blended_user_id) as total,
  min(session_start_tstamp) over (partition by blended_user_id) as first_event_date
  from {{ref('sessions_enriched')}} s
  join {{ref('users')}} users on users.id = s.user_id
  where
  (s.session_start_tstamp <= users.created_at or s.dvce_min_tstamp <= users.created_at)
),
points_attributed as (
  select *,
  session_number || ' of ' || total as attribution_type,
  case when total = 1 then 1.0
       when total = 2 then 0.5
       when session_number = 1 then 0.4
       when session_number = total then 0.4
       else 0.2 / (total - 2)
  end as attribution_points
  from session_ranks
)

select session_start_tstamp as timestamp, user_id, channel, source, campaign, attribution_type, attribution_points
from points_attributed
