
select visitor_id,
    last_value(user_id ignore nulls) over (partition by visitor_id order by "timestamp" rows between unbounded preceding and unbounded following) as user_id,
    first_value(channel) over (partition by visitor_id order by "timestamp" rows between unbounded preceding and unbounded following) as first_touch_channel,
    first_value(medium) over (partition by visitor_id order by "timestamp" rows between unbounded preceding and unbounded following) as first_touch_medium,
    first_value(source) over (partition by visitor_id order by "timestamp" rows between unbounded preceding and unbounded following) as first_touch_source,
    first_value(landing_page) over (partition by visitor_id order by "timestamp" rows between unbounded preceding and unbounded following) as first_landing_page,
    first_value(referrer_url) over (partition by visitor_id order by "timestamp" rows between unbounded preceding and unbounded following) as first_referrer_url,

    last_value(channel) over (partition by visitor_id order by "timestamp" rows between unbounded preceding and unbounded following) as last_touch_channel,
    last_value(medium) over (partition by visitor_id order by "timestamp" rows between unbounded preceding and unbounded following) as last_touch_medium,
    last_value(source) over (partition by visitor_id order by "timestamp" rows between unbounded preceding and unbounded following) as last_touch_source,
    last_value(landing_page) over (partition by visitor_id order by "timestamp" rows between unbounded preceding and unbounded following) as last_landing_page,
    last_value(referrer_url) over (partition by visitor_id order by "timestamp" rows between unbounded preceding and unbounded following) as last_referrer_url,

    listagg(channel, ' | ') within group (order by timestamp) over (partition by visitor_id) as all_channels,
    listagg(medium, ' | ') within group (order by timestamp) over (partition by visitor_id) as all_mediums,
    listagg(source, ' | ') within group (order by timestamp) over (partition by visitor_id) as all_sources,

    min(timestamp) over (partition by visitor_id order by timestamp rows between unbounded preceding and unbounded following) as first_touch_timestamp,
    max(timestamp) over (partition by visitor_id order by timestamp rows between unbounded preceding and unbounded following) as last_touch_timestamp,

    count(session_id) over (partition by visitor_id) as count_sessions

from {{ ref('multitouch_timeseries') }}
