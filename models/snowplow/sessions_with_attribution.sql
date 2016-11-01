
{{ config(
    materialized="table",
    sort="session_start_tstamp",
    dist="domain_userid"
  )
}}


with sessions as (

    select * from {{ ref('sessions_with_channels') }}

),

users as (

    select * from {{ ref('users') }}

),

pre_user_creation_sessions as (
  select
    s.*,

    replace(replace(lower(landing_page_host || landing_page_path), 'http://', ''), 'https://', '') as landing_page,
    replace(replace(lower(exit_page_host || exit_page_path), 'http://', ''), 'https://', '') as exit_page,
    replace(replace(lower(refr_urlhost || refr_urlpath), 'http://', ''), 'https://', '') as referrer_url,
    blended_sessionidx as session_number,
    count(*) over (partition by s.blended_user_id) as sessions_count

    from sessions s
        left outer join users on users.id = s.inferred_user_id
    where (
        s.session_start_tstamp <= users.created_at or
        s.min_dvce_created_tstamp <= users.created_at or
        users.id is null
    )
),

points_attributed as (
    select *,
    session_number || ' of ' || sessions_count as attribution_type,

    case when inferred_user_id is null then 0.0 -- don't attribute points if the visitor didn't sign up
        when sessions_count = 1 then 1.0
        when sessions_count = 2 then 0.5
        when session_number = 1 then 0.4
        when session_number = sessions_count then 0.4
        else 0.2 / (sessions_count - 2)
    end as attribution_points

    from pre_user_creation_sessions

)

select * from points_attributed where channel not ilike 'exclude'
