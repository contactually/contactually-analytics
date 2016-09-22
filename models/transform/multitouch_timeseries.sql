with session_ranks as (
  select
  md5(blended_user_id) as visitor_id,
  users.id as user_id,
  domain_userid || '-' || domain_sessionidx as session_id,
  rank() over (partition by blended_user_id order by session_start_tstamp) as session_number,
  session_start_tstamp,
  session_end_tstamp,
  replace(replace(lower(landing_page_host || landing_page_path), 'http://', ''), 'https://', '') as landing_page,
  replace(replace(lower(exit_page_host || exit_page_path), 'http://', ''), 'https://', '') as exit_page,
  event_count,
  user_logged_in as user_logged_in_during_session,
  coalesce(channel, 'Direct') as channel,
  replace(replace(replace(lower(source), '%20', ' '), '+', ' '), '%7c', '|') as source,
  replace(replace(replace(lower(mkt_medium), '%20', ' '), '+', ' '), '%7c', '|') as medium,
  replace(replace(replace(lower(mkt_campaign), '%20', ' '), '+', ' '), '%7c', '|') as campaign,
  replace(replace(replace(lower(mkt_term), '%20', ' '), '+', ' '), '%7c', '|') as term,
  replace(replace(replace(lower(mkt_content), '%20', ' '), '+', ' '), '%7c', '|') as "content",
  count(*) over (partition by blended_user_id) as sessions_count,
  mkt_medium, mkt_source, mkt_campaign, mkt_term, mkt_content,
  min(session_start_tstamp) over (partition by blended_user_id) as first_touch_timestamp,
  max(session_end_tstamp) over (partition by blended_user_id) as last_touch_timestamp,
  users.team_id,
  users.payment_account_id,
  coalesce(users.user_added_after_team_paid, false) as user_added_after_team_paid
  from {{ref('sessions_enriched')}} s
  left outer join {{ref('enriched_users')}} users on users.id = s.user_id
  where
  (s.session_start_tstamp <= users.team_created_at or s.dvce_min_tstamp <= users.team_created_at or users.id is null)
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

), transformed as (

    select session_start_tstamp as timestamp,
           session_number,
           sessions_count,
           session_id,
           visitor_id,
           landing_page,
           exit_page,
           user_id,
           channel,
           source,
           medium,
           campaign,
           term,
           "content",
           attribution_type,
           attribution_points,

           replace(replace(replace(lower(nullif(mkt_medium, '')), '%20', ' '), '+', ' '), '%7c', '|') as original_mkt_medium,
           replace(replace(replace(lower(nullif(mkt_source, '')), '%20', ' '), '+', ' '), '%7c', '|') as original_mkt_source,
           replace(replace(replace(lower(nullif(mkt_campaign, '')), '%20', ' '), '+', ' '), '%7c', '|') as original_mkt_campaign,
           replace(replace(replace(lower(nullif(mkt_term, '')), '%20', ' '), '+', ' '), '%7c', '|') as original_mkt_term,
           replace(replace(replace(lower(nullif(mkt_content, '')), '%20', ' '), '+', ' '), '%7c', '|') as original_mkt_content,

           team_id,
           payment_account_id,
           user_added_after_team_paid

    from points_attributed

), with_ad_id as (
    select
        ads_rollup.id as ad_id,
        transformed.*,
        row_number() over (partition by session_id) as row_number -- TODO : add order by?
    from transformed
    left outer join {{ ref('ads_rollup') }} as ads_rollup ON
        (transformed.landing_page = ads_rollup.base_url OR ads_rollup.base_url is null) AND
        --(transformed.timestamp >= ads_rollup.min_date AND transformed.timestamp <= ads_rollup.max_date) AND
        -- we could check betweenness, but if the utm params match up and it occurred after the ad was placed, then it should count!
        --(transformed.timestamp >= ads_rollup.min_date) AND
        (ads_rollup.original_utm_source ilike transformed.original_mkt_source or ads_rollup.original_utm_source is null) AND
        (ads_rollup.original_utm_medium ilike transformed.original_mkt_medium or ads_rollup.original_utm_medium is null) AND
        (ads_rollup.original_utm_campaign ilike transformed.original_mkt_campaign or ads_rollup.original_utm_campaign is null) AND
        (ads_rollup.original_utm_term ilike transformed.original_mkt_term or ads_rollup.original_utm_term is null) AND
        (ads_rollup.original_utm_content ilike transformed.original_mkt_content or ads_rollup.original_utm_content is null) AND
        NOT (
          ads_rollup.original_utm_medium is null AND
          ads_rollup.original_utm_source is null AND
          ads_rollup.original_utm_campaign is null AND
          ads_rollup.original_utm_term is null AND
          ads_rollup.original_utm_content is null
        )
)

select * from with_ad_id
where row_number = 1
