
with snowplow_referers as (
	select refr_medium, refr_urlhost
	from {{ ref('events') }}
	where refr_medium is not null and refr_urlhost is not null
	group by 1,2
),
formatted_referers as (
	select
	case
		when refr_medium = 'search' then 'Organic'
		when refr_medium = 'social' then 'Social'
		when refr_medium = 'email' then 'Email'
		else refr_medium
	end as channel,
	refr_urlhost,
    replace(refr_urlhost, 'www.', '') as refr_urlhost_clean
	from snowplow_referers
)
select channel, refr_urlhost, refr_urlhost_clean from formatted_referers
