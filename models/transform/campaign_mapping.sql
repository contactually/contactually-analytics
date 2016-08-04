with attribution as (
  select
      "first campaign" as first_campaign,
      "First Source / Medium" as first_src_med,
      "Channel" as channel
  from "dbt_dbanin"."campaigns"
), chan_mapping as (
  select
      replace(lower(trim(split_part(first_campaign, '/', 1))), '+', ' ') as campaign,
      replace(lower(trim(split_part(first_src_med, '/', 1))), '+', ' ')  as source,
      replace(lower(trim(split_part(first_src_med, '/', 2))), '+', ' ')  as medium,
      channel
  from attribution
  group by 1, 2, 3, 4
), formatted_chan_mapping as (
  select
      case when campaign = '(none)' then null else campaign end as campaign,
      case when medium   = '(none)' then null else medium end as medium,
      case when source   = '(none)' then null else source end as source,
      channel
  from chan_mapping
), non_attributable_sources as (
    select source, medium, campaign
    from formatted_chan_mapping
    group by 1, 2, 3
    having count(*) > 1
), filtered_mapped_channels as (
    select * from formatted_chan_mapping
    where source not in (select source from non_attributable_sources)
    and channel != '* EXCLUDE'
)

select * from filtered_mapped_channels
