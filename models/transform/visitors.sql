select visitor_id,
first_value(channel) over (partition by visitor_id order by session_start_tstamp) as first_touch_channel,
first_value(source) over (partition by visitor_id order by session_start_tstamp) as first_touch_source,
first_value(medium) over (partition by visitor_id order by session_start_tstamp) as first_touch_medium,
last_value(channel) over (partition by visitor_id order by session_start_tstamp) as last_touch_channel,
last_value(source) over (partition by visitor_id order by session_start_tstamp) as last_touch_source,
last_value(medium) over (partition by visitor_id order by session_start_tstamp) as last_touch_medium,
count(distinct session_id) as count_
