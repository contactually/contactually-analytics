with pre_trial_attribution as (
    select *
    from {{ ref('pre_trial_attribution') }}
),
  smc_mapping as (
    select distinct
      map.in_source,
      map.in_medium,
      map.in_campaign,
      map.out_channel,
      map.out_source,
      map.out_medium,
      map.out_campaign,
      map.smc_key
    from fivetran_uploads.snowplow_mapping map
),
  snowplow_sessions_with_key as (
    select distinct
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
      pre_trial_attribution.middle_touch_sources,
      pre_trial_attribution.middle_touch_mediums,
      pre_trial_attribution.middle_touch_campaigns,
      pre_trial_attribution.middle_touch_referers,
      pre_trial_attribution.middle_touch_landing_pages,
      case when pre_trial_attribution.first_touch_in_source is not null or pre_trial_attribution.first_touch_in_medium is not null or pre_trial_attribution.first_touch_in_campaign is not null or pre_trial_attribution.first_touch_in_referer is not null
        then lower(nvl( pre_trial_attribution.first_touch_in_source,'' ) || nvl( pre_trial_attribution.first_touch_in_medium,'' ) || nvl( pre_trial_attribution.first_touch_in_campaign,'') || nvl( pre_trial_attribution.first_touch_in_referer,''))
      else null
      end as first_touch_smc_key,
      case when pre_trial_attribution.last_touch_in_source is not null or pre_trial_attribution.last_touch_in_medium is not null or pre_trial_attribution.last_touch_in_campaign is not null
        then lower(nvl(pre_trial_attribution.last_touch_in_source, '') || nvl(pre_trial_attribution.last_touch_in_medium, '') || nvl(pre_trial_attribution.last_touch_in_campaign, ''))
      else null
      end as last_touch_smc_key
    from pre_trial_attribution
)
select distinct
  sp_sessions.blended_user_id,
  sp_sessions.first_touch_date,
  sp_sessions.last_touch_date,
  sp_sessions.session_count,
  sp_sessions.bounced_sessions,
  sp_sessions.engaged_sessions,
  sp_sessions.time_on_site_in_s,
  sp_sessions.page_view_count,
  sp_sessions.first_touch_in_source,
  sp_sessions.first_touch_in_medium,
  sp_sessions.first_touch_in_campaign,
  sp_sessions.first_touch_in_referer,
  sp_sessions.last_touch_in_source,
  sp_sessions.last_touch_in_medium,
  sp_sessions.last_touch_in_campaign,
  sp_sessions.last_touch_in_referer,
  nvl(first_touch_mapping.out_channel,
      case when sp_sessions.first_touch_smc_key is null
        then 'direct'
      end) as first_touch_out_channel,
  first_touch_mapping.out_source as first_touch_out_source,
  first_touch_mapping.out_medium as first_touch_out_medium,
  first_touch_mapping.out_campaign as first_touch_out_campaign,
  sp_sessions.first_touch_landing_page,
  nvl(last_touch_mapping.out_channel,
      case when sp_sessions.last_touch_smc_key is null
        then 'direct'
      end) as last_touch_out_channel,
  last_touch_mapping.out_source as last_touch_out_source,
  last_touch_mapping.out_medium as last_touch_out_medium,
  last_touch_mapping.out_campaign as last_touch_out_campaign,
  sp_sessions.last_touch_landing_page,
  sp_sessions.middle_touch_sources,
  sp_sessions.middle_touch_mediums,
  sp_sessions.middle_touch_campaigns,
  sp_sessions.middle_touch_referers,
  sp_sessions.middle_touch_landing_pages
from snowplow_sessions_with_key sp_sessions
left join smc_mapping first_touch_mapping
  on sp_sessions.first_touch_smc_key = lower(first_touch_mapping.smc_key)
left join smc_mapping last_touch_mapping
  on sp_sessions.last_touch_smc_key = lower(last_touch_mapping.smc_key)
