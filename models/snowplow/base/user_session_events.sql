with snowplow_user_id_map as
(select distinct
   events.user_id,
   users.created_at,
   payment_accounts.first_charged_at,
   events.domain_userid
 from snowplow.event events
   left join postgres_public.users users
     on events.user_id = users.id
   left join postgres_public.teams teams
     on users.team_id = teams.id
   left join postgres_public.payment_accounts payment_accounts
     on teams.payment_account_id = payment_accounts.id
 where events.event not in ('pp','pv')
       and events.user_id is not null
 order by events.user_id),
    unranked_sessions as (
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
          where events.event in ('pp','pv')
          group by 1,2
        ) session_times
          on events.domain_userid = session_times.domain_userid
             and events.domain_sessionidx = session_times.domain_sessionidx
        left join snowplow_user_id_map map
          on events.domain_userid = map.domain_userid
      where events.event in ('pp','pv')
            and events.collector_tstamp >= '2017-01-01'
      order by events.collector_tstamp
  ),
    calculated_session_idx as (
      select
        sessions.blended_user_id,
        sessions.domain_sessionid,
        sessions.session_start,
        row_number() over (partition by blended_user_id order by session_start) as session_index
      from
        (select distinct
           blended_user_id,
           domain_sessionid,
           session_start
         from
           unranked_sessions) sessions)
select
  unranked_sessions.domain_userid,
  unranked_sessions.user_id,
  unranked_sessions.blended_user_id,
  unranked_sessions.pre_trial_session_flag,
  unranked_sessions.pre_customer_session_flag,
  unranked_sessions.domain_sessionid,
  /*******SESSION IDX HAS TO BE RECALCULATED FOR USERS TO ROLL UP MULTIPLE SNOWPLOW IDS UNDER THE SAME USER*******/
  --unranked_sessions.domain_sessionidx,
  calculated_session_idx.session_index as domain_sessionidx,
  /***************************************************************************************************************/
  unranked_sessions.session_start,
  unranked_sessions.session_end,
  unranked_sessions.event_duration_in_s,
  unranked_sessions.event_id,
  unranked_sessions.user_event_index,
  unranked_sessions.session_event_index,
  unranked_sessions.collector_tstamp,
  unranked_sessions.dvce_tstamp,
  unranked_sessions.event,
  unranked_sessions.page_url,
  unranked_sessions.page_urlscheme,
  unranked_sessions.page_urlhost,
  unranked_sessions.page_urlport,
  unranked_sessions.page_urlpath,
  unranked_sessions.page_urlquery,
  unranked_sessions.page_urlfragment,
  unranked_sessions.page_title,
  unranked_sessions.referer_url,
  unranked_sessions.refr_urlscheme,
  unranked_sessions.refr_urlhost,
  unranked_sessions.refr_urlport,
  unranked_sessions.refr_urlpath,
  unranked_sessions.refr_urlquery,
  unranked_sessions.refr_urlfragment,
  unranked_sessions.refr_medium,
  unranked_sessions.refr_source,
  unranked_sessions.refr_term,
  unranked_sessions.mkt_campaign,
  unranked_sessions.mkt_content,
  unranked_sessions.mkt_medium,
  unranked_sessions.mkt_source,
  unranked_sessions.mkt_term,
  unranked_sessions.app_id
from unranked_sessions
  inner join calculated_session_idx
    on unranked_sessions.blended_user_id = calculated_session_idx.blended_user_id
       and unranked_sessions.domain_sessionid = calculated_session_idx.domain_sessionid
