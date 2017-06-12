{{ config(
materialized='table',
sort=['first_touch_date', 'last_touch_date'],
dist='blended_user_id'
)
}}

with user_session_events as (
    select * from {{ ref('user_session_events') }}
),
    pre_customer_session_indexes as (
      select
        events.blended_user_id,
        min(events.user_event_index) as min_user_event_index,
        max(events.user_event_index) as max_user_event_index,
        max(events.domain_sessionidx) as max_session_index,
        min(events.domain_sessionidx) as min_session_index
      from
        user_session_events events
      where events.pre_customer_session_flag = 1
      group by 1),
    pre_customer_session_totals as
  (select
     blended_user_id,
     sum(session_duration_in_s) as time_on_site_in_s,
     sum(case when session_duration_in_s = 0 then 1 else 0 end) as bounced_sessions,
     sum(case when session_duration_in_s >= 30 then 1 else 0 end) as engaged_sessions,
     sum(page_view_count) as page_view_count,
     max(domain_sessionidx) as session_count
   from
     (
       select
         events.blended_user_id,
         events.domain_sessionidx,
         sum( events.event_duration_in_s ) as session_duration_in_s,
         sum( case when events.event = 'pv'
           then 1
              else 0 end ) as page_view_count
       from
         user_session_events events
       where events.pre_customer_session_flag = 1
       group by 1,2)
   group by 1
  )
select distinct
  base.blended_user_id,
  totals.session_count,
  totals.bounced_sessions,
  totals.engaged_sessions,
  totals.time_on_site_in_s,
  totals.page_view_count,
  /********FIRST TOUCH********/
  first_touch.collector_tstamp as first_touch_date,
  case when first_touch.mkt_medium is not null
    then first_touch.mkt_medium
  else first_touch.refr_medium
  end as first_touch_in_medium,
  case when first_touch.mkt_source is not null
    then first_touch.mkt_source
  else first_touch.refr_source
  end as first_touch_in_source,
  first_touch.mkt_campaign as first_touch_in_campaign,
  first_touch.referer_url as first_touch_in_referer,
  first_touch.page_url as first_touch_landing_page,
  /********LAST TOUCH********/
  last_touch.collector_tstamp as last_touch_date,
  case when last_touch.mkt_medium is not null
    then last_touch.mkt_medium
  else last_touch.refr_medium
  end as last_touch_in_medium,
  case when last_touch.mkt_source is not null
    then last_touch.mkt_source
  else last_touch.refr_source
  end as last_touch_in_source,
  last_touch.mkt_campaign as last_touch_in_campaign,
  last_touch.referer_url as last_touch_in_referer,
  last_touch.page_url as last_touch_landing_page,
  /********MIDDLE TOUCH********/
  listagg(middle_touch.mkt_medium, ',') within group (order by middle_touch.domain_sessionidx) as middle_touch_mediums,
  listagg(middle_touch.mkt_source, ',') within group (order by middle_touch.domain_sessionidx) as middle_touch_sources,
  listagg(middle_touch.mkt_campaign, ',') within group (order by middle_touch.domain_sessionidx) as middle_touch_campaigns,
  listagg(middle_touch.referer_url, ',') within group (order by middle_touch.domain_sessionidx) as middle_touch_referers,
  listagg(middle_touch.page_url, ',') within group (order by middle_touch.domain_sessionidx) as middle_touch_landing_pages
from (select distinct blended_user_id
      from user_session_events
     )base
  inner join pre_customer_session_indexes indexes
    on base.blended_user_id = indexes.blended_user_id
  inner join user_session_events first_touch
    on indexes.blended_user_id = first_touch.blended_user_id
       and first_touch.domain_sessionidx = indexes.min_session_index
       and first_touch.session_event_index = 1
       and first_touch.pre_customer_session_flag = 1
  left join user_session_events last_touch
    on indexes.blended_user_id = last_touch.blended_user_id
       and last_touch.domain_sessionidx = indexes.max_session_index
       and last_touch.session_event_index = 1
       and last_touch.pre_customer_session_flag = 1
  left join user_session_events middle_touch
    on indexes.blended_user_id = middle_touch.blended_user_id
       and middle_touch.domain_sessionidx > indexes.min_session_index
       and middle_touch.domain_sessionidx < indexes.max_session_index
       and middle_touch.session_event_index = 1
       and middle_touch.pre_customer_session_flag = 1
  inner join pre_customer_session_totals totals
    on totals.blended_user_id = indexes.blended_user_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18