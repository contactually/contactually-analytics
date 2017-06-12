{{ config(
materialized='table',
sort=['session_id', 'ad_id'],
dist='session_id'
)
}}

with sessions as (
    select
      session_id,
      first_touch_date :: DATE as date,
      first_touch_out_channel as out_channel,
      first_touch_out_source as out_source,
      first_touch_out_medium as out_medium,
      first_touch_out_campaign as out_campaign
    from analytics.pre_customer_sessions_with_mappings
),
    ads as (
      select
        ad_id,
        date :: DATE as date,
        out_channel,
        out_source,
        out_medium,
        out_campaign
      from analytics.ads_base_with_mappings
  )
select distinct
  ads.ad_id,
  sessions.session_id
from ads
  inner join sessions
    on ads.date = sessions.date
       and ads.out_channel = sessions.out_channel
       and ads.out_source = sessions.out_source
       and ads.out_medium = sessions.out_medium
       and ads.out_campaign = sessions.out_campaign