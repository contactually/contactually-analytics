with session_ranks as (
  select
  md5(blended_user_id) as visitor_id,
  users.id as user_id,
  domain_userid || '-' || domain_sessionidx as session_id,
  rank() over (partition by blended_user_id order by session_start_tstamp) as session_number,
  session_start_tstamp,
  session_end_tstamp,
  event_count,
  user_logged_in as user_logged_in_during_session,
  coalesce(channel, 'Direct') as channel,
  lower(replace(replace(source, '%20', ' '), '+', ' ')) as source,
  lower(replace(replace(mkt_medium, '%20', ' '), '+', ' ')) as medium,
  lower(replace(replace(mkt_campaign, '%20', ' '), '+', ' ')) as campaign,
  lower(replace(replace(mkt_term, '%20', ' '), '+', ' ')) as term,
  lower(replace(replace(mkt_content, '%20', ' '), '+', ' ')) as "content",
  count(*) over (partition by blended_user_id) as sessions_count,
  min(session_start_tstamp) over (partition by blended_user_id) as first_touch_timestamp,
  max(session_end_tstamp) over (partition by blended_user_id) as last_touch_timestamp
  from {{ref('sessions_enriched')}} s
  left outer join {{ref('users')}} users on users.id = s.user_id
  where
  (s.session_start_tstamp <= users.created_at or s.dvce_min_tstamp <= users.created_at or users.id is null)
),
points_attributed as (
  select *,
  session_number || ' of ' || sessions_count as attribution_type,
case when user_id is null then 0.0 -- don't attribute points if the visitor didn't sign up
       when sessions_count = 1 then 1.0
       when sessions_count = 2 then 0.5
       when session_number = 1 then 0.4
       when session_number = sessions_count then 0.4
       else 0.2 / (sessions_count - 2)
  end as attribution_points
  from session_ranks
)

select session_start_tstamp as timestamp,
       session_number,
       sessions_count,
       session_id,
       visitor_id,
       user_id,
       channel,
       source,
       medium,
       campaign,
       term,
       "content",
       attribution_type,
       attribution_points
from points_attributed
