--right now, this makes the assumption that your session cookie should be associated with the most recent user_id used for it.
--if there have been multiple, the earlier ones are ignored and only the most recent one is used.


with events as (

  select * from {{ref('product_events')}}

), d1 as (

  select
    domain_userid,
    last_value(se_action) over
      (partition by domain_userid order by collector_tstamp rows between unbounded preceding and unbounded following)
      as user_id
  from events

)

select *
from d1
group by 1, 2
