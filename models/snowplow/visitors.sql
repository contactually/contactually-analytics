
{{
    config(
        materialized='table',
        sort='blended_user_id',
        dist='blended_user_id'
    )
}}

with visitors as (

    select * from {{ ref('snowplow_visitors') }}

),

sessions as (

    select * from {{ ref('sessions_with_attribution') }}

),

users as (

    select * from {{ ref('enriched_users') }}

),

pre_signup_sessions as (

    select sessions.*,
           count(*) over (partition by sessions.blended_user_id) as __session_count,
           row_number() over (partition by sessions.blended_user_id order by sessions.session_start_tstamp) as __session_index
    from sessions
        join visitors on visitors.blended_user_id = sessions.blended_user_id
        left outer join users on users.id = sessions.inferred_user_id
    where (
        sessions.session_start_tstamp <= users.created_at or
        sessions.min_dvce_created_tstamp <= users.created_at or
        users.id is null
    )

),

-- first session this visitor had
first_touch as (

    select s.blended_user_id,
           s.channel      as first_touch_channel,
           s.medium       as first_touch_medium,
           s.source       as first_touch_source,
           s.campaign     as first_touch_campaign,
           s.landing_page as first_touch_landing_page,
           s.referrer_url as first_referrer_url
    from pre_signup_sessions s
    where s.__session_index = 1

),

-- last session before this visitor converted to a user. will be null for unconverted users
last_touch as (

    select s.blended_user_id,
           s.channel      as last_touch_channel,
           s.medium       as last_touch_medium,
           s.source       as last_touch_source,
           s.campaign     as last_touch_campaign,
           s.landing_page as last_touch_landing_page,
           s.referrer_url as last_referrer_url
    from pre_signup_sessions s
    where s.__session_index = s.__session_count

),

middle_touches as (

    select distinct
        blended_user_id,

        listagg(channel, ' | ')      within group (order by session_start_tstamp) over (partition by blended_user_id) as middle_channels,
        listagg(medium, ' | ')       within group (order by session_start_tstamp) over (partition by blended_user_id) as middle_mediums,
        listagg(source, ' | ')       within group (order by session_start_tstamp) over (partition by blended_user_id) as middle_sources,
        listagg(campaign, ' | ')     within group (order by session_start_tstamp) over (partition by blended_user_id) as middle_campaigns,
        listagg(landing_page, ' | ') within group (order by session_start_tstamp) over (partition by blended_user_id) as middle_landing_pages,
        listagg(referrer_url, ' | ') within group (order by session_start_tstamp) over (partition by blended_user_id) as middle_referrer_urls
    from pre_signup_sessions
    where __session_index > 1 and __session_index < __session_count

)

select visitors.*,

       first_touch.first_touch_channel,
       first_touch.first_touch_medium,
       first_touch.first_touch_source,
       first_touch.first_touch_campaign,
       first_touch.first_touch_landing_page,
       first_touch.first_referrer_url,


       last_touch.last_touch_channel,
       last_touch.last_touch_medium,
       last_touch.last_touch_source,
       last_touch.last_touch_campaign,
       last_touch.last_touch_landing_page,
       last_touch.last_referrer_url,

       middle_touches.middle_channels,
       middle_touches.middle_mediums,
       middle_touches.middle_sources,
       middle_touches.middle_campaigns,
       middle_touches.middle_landing_pages,
       middle_touches.middle_referrer_urls

from visitors
    left outer join first_touch on first_touch.blended_user_id = visitors.blended_user_id
    left outer join last_touch on last_touch.blended_user_id = visitors.blended_user_id
    left outer join middle_touches on middle_touches.blended_user_id = visitors.blended_user_id

