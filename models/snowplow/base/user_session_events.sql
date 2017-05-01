with snowplow_user_id_map as
(select distinct
   events.user_id,
   users.created_at,
   events.domain_userid
 from snowplow.event events
   left join postgres_public.users users
     on events.user_id = users.id
 where events.event not in ('pp','pv')
       and events.user_id is not null
 order by events.user_id)
select
  events.domain_userid,
  map.user_id,
  case when map.user_id is not null
    then map.user_id
  else events.domain_userid
  end as blended_user_id,
  map.created_at as user_created_at,
  case when map.user_id is null or session_start < map.created_at
    then 1
  else 0
  end as pre_trial_session_flag,
  events.domain_sessionid,
  events.domain_sessionidx,
  session_times.session_start,
  session_times.session_end,
  coalesce(datediff(second,lag(events.collector_tstamp,1) over (partition by events.domain_userid, events.domain_sessionidx order by events.collector_tstamp),events.collector_tstamp),0) as event_duration_in_s,
  events.event_id,
  row_number() over (partition by events.domain_userid order by events.collector_tstamp) as user_event_index,
  row_number() over (partition by events.domain_userid, events.domain_sessionid order by events.collector_tstamp) as session_event_index,
  events.collector_tstamp,
  events.dvce_tstamp,
  events.event,
  events.page_url,
  events.page_urlscheme,
  events.page_urlhost,
  events.page_urlport,
  events.page_urlpath,
  events.page_urlquery,
  events.page_urlfragment,
  events.page_title,
  events.refr_urlhost || events.refr_urlpath as referer_url,
  events.refr_urlscheme,
  events.refr_urlhost,
  events.refr_urlport,
  events.refr_urlpath,
  events.refr_urlquery,
  events.refr_urlfragment,
  events.refr_medium,
  events.refr_source,
  events.refr_term,
  events.mkt_campaign,
  events.mkt_content,
  events.mkt_medium,
  events.mkt_source,
  events.mkt_term,
  events.app_id
from snowplow.event events
  inner join
  (
    select
      events.domain_userid,
      events.domain_sessionidx,
      min( events.collector_tstamp ) as session_start,
      max( events.collector_tstamp ) as session_end
    from
      snowplow.event events
    where events.event in ('pp', 'pv')
    group by 1,2
  )session_times
    on events.domain_userid = session_times.domain_userid
       and events.domain_sessionidx = session_times.domain_sessionidx
  left join snowplow_user_id_map map
    on events.domain_userid = map.domain_userid
where events.event in ('pp', 'pv')
      and events.collector_tstamp >= '2017-01-01'
order by events.collector_tstamp