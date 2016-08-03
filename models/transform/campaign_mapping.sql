with attribution as (
	select
	"first campaign" as first_campaign,
	"First Source / Medium" as first_src_med,
	"Channel" as channel
	from dbt_dbanin.campaigns
), chan_mapping as (
	select
	lower(trim(split_part(first_campaign, '/', 1))) as campaign,
	lower(trim(split_part(first_src_med, '/', 1))) as source,
	lower(trim(split_part(first_src_med, '/', 2))) as medium,
	channel,
	count(*)
	from attribution
	group by 1, 2, 3,4
	order by 5 desc
), formatted_chan_mapping as (
	select
	case when campaign = '(none)' then null else campaign end as campaign,
	case when medium   = '(none)' then null else medium end as medium,
	case when source   = '(none)' then null else source end as source,
	channel
	from chan_mapping
)

select * from formatted_chan_mapping
