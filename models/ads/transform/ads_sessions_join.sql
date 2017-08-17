with user_info as (
    select
      users.id,
      case when payment_accounts.id is not null
        then 1
      else 0
      end as customer_flag,
      case when payment_accounts.status in (2,3)
        then 1
      else 0
      end as paying_flag
    from postgres_public.users users
      inner join postgres_public.teams teams
        on users.team_id = teams.id
      left join postgres_public.payment_accounts payment_accounts
        on teams.payment_account_id = payment_accounts.id
),
    sessions_stats as (
      select
        first_touch_date :: DATE as date,
        first_touch_out_channel as out_channel,
        first_touch_out_source as out_source,
        first_touch_out_medium as out_medium,
        first_touch_out_campaign as out_campaign,
        count( distinct blended_user_id ) as visitor_count,
        sum( case when user_info.id is not null
          then 1
             else 0 end ) as user_count,
        sum( case when user_info.customer_flag is not null then user_info.customer_flag else 0 end ) as customer_count
      from analytics.pre_customer_sessions_with_mappings sp
        left join user_info
          on sp.blended_user_id = user_info.id
      group by 1,2,3,4,5
  ),
    sessions as (
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
  sessions.session_id,
  sessions_stats.visitor_count,
  sessions_stats.user_count,
  sessions_stats.customer_count
from ads
  inner join sessions
    on ads.date = sessions.date
       and ads.out_channel = sessions.out_channel
       and ads.out_source = sessions.out_source
       and ads.out_medium = sessions.out_medium
       and ads.out_campaign = sessions.out_campaign
  left join sessions_stats
    on ads.date = sessions_stats.date
       and ads.out_channel = sessions_stats.out_channel
       and ads.out_source = sessions_stats.out_source
       and ads.out_medium = sessions_stats.out_medium
       and ads.out_campaign = sessions_stats.out_campaign