with snowplow_user_id_map as
(
    select distinct
      case when events.user_id ilike 'user_%'
        then substring(events.user_id,6,length(events.user_id))
        else events.user_id
      end as user_id,
      users.created_at,
      payment_accounts.first_charged_at,
      events.domain_userid
    from snowplow.event events
      left join postgres_public_production_main_public.users users
        on events.user_id = users.id
      left join postgres_public_production_main_public.teams teams
        on users.team_id = teams.id
      left join postgres_public_production_main_public.payment_accounts payment_accounts
        on teams.payment_account_id = payment_accounts.id
    where events.event not in ('pp','pv')
          and events.user_id is not null
    order by events.user_id
)
select
  events.domain_userid,
  map.user_id,
  case when map.user_id is not null
    then map.user_id
  else events.domain_userid
  end as blended_user_id,
  map.created_at as user_created_at,
  case when map.user_id is null or (session_start - '120 seconds'::INTERVAL) < map.created_at
    then 1
  else 0
  end as pre_trial_session_flag,
  case when (map.user_id is null) or (map.user_id is not null and map.first_charged_at is null) or
            (map.first_charged_at is not null and session_start < map.first_charged_at)
    then 1
  else 0
  end as pre_customer_session_flag,
  events.domain_sessionid,
  events.domain_sessionidx,
  session_times.session_start,
  session_times.session_end,
  coalesce( datediff( second,lag( events.collector_tstamp,1 )
  over (
    partition by events.domain_userid,events.domain_sessionidx
    order by events.collector_tstamp ),events.collector_tstamp ),0 ) as event_duration_in_s,
  events.event_id,
  row_number( )
  over (
    partition by case when map.user_id is not null
      then map.user_id
                 else events.domain_userid end
    order by events.collector_tstamp ) as user_event_index,
  row_number( )
  over (
    partition by case when map.user_id is not null
      then map.user_id
                 else events.domain_userid end,events.domain_sessionid
    order by events.collector_tstamp ) as session_event_index,
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
  events.app_id,
  website_redesign.website_version_seen
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
    where events.event in ('pp','pv')
    group by 1,2
  )session_times
    on events.domain_userid = session_times.domain_userid
       and events.domain_sessionidx = session_times.domain_sessionidx
  left join snowplow_user_id_map map
    on events.domain_userid = map.domain_userid
  left join
  (
    select
      event.domain_userid,
      max(event.se_label) as website_version_seen
    from snowplow.event event
    where event.se_category = 'ab_testing'
          and event.se_action = 'website_redesign'
          and event.event = 'se'
          and event.se_label is not null
    group by 1
  )website_redesign
    on website_redesign.domain_userid = events.domain_userid
where events.event in ('pp','pv')
      and events.collector_tstamp >= '2017-01-01'