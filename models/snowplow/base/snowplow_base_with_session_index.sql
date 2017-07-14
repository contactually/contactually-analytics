select
  sp_base_events.domain_userid,
  sp_base_events.user_id,
  sp_base_events.blended_user_id,
  sp_base_events.pre_trial_session_flag,
  sp_base_events.pre_customer_session_flag,
  sp_base_events.domain_sessionid,
  /*******SESSION IDX HAS TO BE RECALCULATED FOR USERS TO ROLL UP MULTIPLE SNOWPLOW IDS UNDER THE SAME USER*******/
  --sp_base_events.domain_sessionidx,
  calculated_session_idx.session_index as domain_sessionidx,
  /***************************************************************************************************************/
  sp_base_events.session_start,
  sp_base_events.session_end,
  sp_base_events.event_duration_in_s,
  sp_base_events.event_id,
  sp_base_events.user_event_index,
  sp_base_events.session_event_index,
  sp_base_events.collector_tstamp,
  sp_base_events.dvce_tstamp,
  sp_base_events.event,
  sp_base_events.page_url,
  sp_base_events.page_urlscheme,
  sp_base_events.page_urlhost,
  sp_base_events.page_urlport,
  sp_base_events.page_urlpath,
  sp_base_events.page_urlquery,
  sp_base_events.page_urlfragment,
  sp_base_events.page_title,
  sp_base_events.referer_url,
  sp_base_events.refr_urlscheme,
  sp_base_events.refr_urlhost,
  sp_base_events.refr_urlport,
  sp_base_events.refr_urlpath,
  sp_base_events.refr_urlquery,
  sp_base_events.refr_urlfragment,
  sp_base_events.refr_medium,
  sp_base_events.refr_source,
  sp_base_events.refr_term,
  sp_base_events.mkt_campaign,
  sp_base_events.mkt_content,
  sp_base_events.mkt_medium,
  sp_base_events.mkt_source,
  sp_base_events.mkt_term,
  sp_base_events.app_id
from analytics.snowplow_base_events sp_base_events
inner join
  (
    select
      sessions.blended_user_id,
      sessions.domain_sessionid,
      sessions.session_start,
      row_number() over (partition by blended_user_id order by session_start) as session_index
    from
      (
        select distinct
          blended_user_id,
          domain_sessionid,
          session_start
        from
          analytics.snowplow_base_events
      )sessions
  )calculated_session_idx
  on sp_base_events.blended_user_id = calculated_session_idx.blended_user_id
  and sp_base_events.domain_sessionid = calculated_session_idx.domain_sessionid