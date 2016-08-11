with events as (

  select * from {{ref('events')}}

)

select
  domain_userid,
  domain_sessionidx
from snowplow.event where se_category = 'identify'
group by 1, 2
