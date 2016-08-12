-- TODO : only use sessions that occur BEFORE the signup happens
-- TODO : limit this to sessions + users > 2016-07-04

with session_ranks as (
  select
  blended_user_id,
  domain_sessionidx,
  channel,
  enriched_medium,
  enriched_source,
  mkt_campaign,
  rank() over (partition by blended_user_id order by domain_sessionidx) as rank,
  count(*) over (partition by blended_user_id) as total
  from {{ ref('sessions_enriched') }}
  where user_id is not null
),
points_attributed as (
  select *,
  case when total = 1 then 1.0
     when total = 2 then 0.5
       when rank = 1 then 0.4
       when rank = total then 0.40
       else 0.2 / (total - 2) end as points
  from session_ranks
)

select channel, enriched_source, mkt_campaign, sum(points) as score from points_attributed group by 1, 2, 3
