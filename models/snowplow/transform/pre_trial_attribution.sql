with user_session_events as (
  select * from {{ ref('user_session_events') }}
),
    pre_trial_session_indexes as
  (select
     events.blended_user_id,
     min(events.user_event_index) as min_user_event_index,
     max(events.user_event_index) as max_user_event_index,
     max(events.domain_sessionidx) as max_session_index
   from
     user_session_events events
   where events.pre_trial_session_flag = 1
   group by 1),
    pre_trial_session_totals as
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
       where events.pre_trial_session_flag = 1
       group by 1,2)
   group by 1
  ),
    pre_trial_attribution as
  (select distinct
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
     last_touch.page_url as last_touch_landing_page
   from (select distinct blended_user_id
         from user_session_events
        )base
     inner join pre_trial_session_indexes indexes
       on base.blended_user_id = indexes.blended_user_id
     inner join user_session_events first_touch
       on indexes.blended_user_id = first_touch.blended_user_id
          and first_touch.domain_sessionidx = 1
          and first_touch.session_event_index = 1
          and first_touch.pre_trial_session_flag = 1
     left join user_session_events last_touch
       on indexes.blended_user_id = last_touch.blended_user_id
          and indexes.max_session_index = last_touch.domain_sessionidx
          and last_touch.session_event_index = indexes.max_user_event_index
          and last_touch.pre_trial_session_flag = 1
     inner join pre_trial_session_totals totals
       on totals.blended_user_id = indexes.blended_user_id)
select
  pre_trial_attribution.blended_user_id,
  pre_trial_attribution.session_count,
  pre_trial_attribution.bounced_sessions,
  pre_trial_attribution.engaged_sessions,
  pre_trial_attribution.time_on_site_in_s,
  pre_trial_attribution.page_view_count,
  pre_trial_attribution.first_touch_date,
  pre_trial_attribution.first_touch_in_medium,
  pre_trial_attribution.first_touch_in_source,
  pre_trial_attribution.first_touch_in_campaign,
  pre_trial_attribution.first_touch_in_referer,
  pre_trial_attribution.first_touch_landing_page,
  pre_trial_attribution.last_touch_date,
  pre_trial_attribution.last_touch_in_medium,
  pre_trial_attribution.last_touch_in_source,
  pre_trial_attribution.last_touch_in_campaign,
  pre_trial_attribution.last_touch_in_referer,
  pre_trial_attribution.last_touch_landing_page,
  /*case when pre_trial_attribution.first_touch_in_source is not null or pre_trial_attribution.first_touch_in_medium is not null or pre_trial_attribution.first_touch_in_campaign is not null
    then lower(nvl(pre_trial_attribution.first_touch_in_source, '') || nvl(pre_trial_attribution.first_touch_in_medium, '') || nvl(pre_trial_attribution.first_touch_in_campaign, ''))
    end as join_key,*/
  max(nvl(first_touch_mapping.out_channel, first_touch_referer_mapping.out_channel,
          case when pre_trial_attribution.first_touch_in_source is null and pre_trial_attribution.first_touch_in_medium is null and pre_trial_attribution.first_touch_in_campaign is null and pre_trial_attribution.first_touch_in_referer is null
            then 'direct'
          end)) as first_touch_out_channel,
  max(nvl(first_touch_mapping.out_source, first_touch_referer_mapping.out_source)) as first_touch_out_source,
  max(nvl(first_touch_mapping.out_medium, first_touch_referer_mapping.out_medium)) as first_touch_out_medium,
  max(nvl(first_touch_mapping.out_campaign, first_touch_referer_mapping.out_campaign)) as first_touch_out_campaign,
  max(nvl(first_touch_mapping.out_channel, first_touch_referer_mapping.out_channel,
          case when pre_trial_attribution.last_touch_in_source is null and pre_trial_attribution.last_touch_in_medium is null and pre_trial_attribution.last_touch_in_campaign is null and pre_trial_attribution.last_touch_in_referer is null
            then 'direct'
          end)) as last_touch_out_channel,
  max(nvl(last_touch_mapping.out_source, last_touch_referer_mapping.out_source)) as last_touch_out_source,
  max(nvl(last_touch_mapping.out_medium, last_touch_referer_mapping.out_medium)) as last_touch_out_medium,
  max(nvl(last_touch_mapping.out_campaign, last_touch_referer_mapping.out_campaign)) as last_touch_out_campaign
from pre_trial_attribution pre_trial_attribution
  left join fivetran_uploads.channel_mapping_v2 first_touch_mapping
    on case when pre_trial_attribution.first_touch_in_source is not null or pre_trial_attribution.first_touch_in_medium is not null or pre_trial_attribution.first_touch_in_campaign is not null
    then lower(nvl(pre_trial_attribution.first_touch_in_source, '') || nvl(pre_trial_attribution.first_touch_in_medium, '') || nvl(pre_trial_attribution.first_touch_in_campaign, ''))
       else null
       end = lower(first_touch_mapping.smc_key)
  left join fivetran_uploads.channel_mapping_v2 first_touch_referer_mapping
    on lower(pre_trial_attribution.first_touch_in_referer) = lower(first_touch_referer_mapping.in_referer)
  left join fivetran_uploads.channel_mapping_v2 last_touch_mapping
    on case when pre_trial_attribution.last_touch_in_source is not null or pre_trial_attribution.last_touch_in_medium is not null or pre_trial_attribution.last_touch_in_campaign is not null
    then lower(nvl(pre_trial_attribution.last_touch_in_source, '') || nvl(pre_trial_attribution.last_touch_in_medium, '') || nvl(pre_trial_attribution.last_touch_in_campaign, ''))
       else null
       end = lower(last_touch_mapping.smc_key)
  left join fivetran_uploads.channel_mapping_v2 last_touch_referer_mapping
    on lower(pre_trial_attribution.last_touch_in_referer) = lower(last_touch_referer_mapping.in_referer)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18