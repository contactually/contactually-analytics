with events as (
  select
    e.*
  from
    {{ref('events')}} as e
  where e.domain_userid is not null -- do not aggregate null
    and e.domain_sessionidx is not null -- do not aggregate null
    and e.domain_userid != '' -- do not aggregate missing domain_userids
    and e.dvce_tstamp is not null -- required, dvce_created_tstamp is used to sort events
    and e.collector_tstamp > '2000-01-01' -- remove incorrect collector_tstamps, can cause sql errors
    and e.collector_tstamp < '2030-01-01' -- remove incorrect collector_tstamps, can cause sql errors
    and e.dvce_tstamp > '2000-01-01' -- remove incorrect dvce_created_tstamps, can cause sql errors
    and e.dvce_tstamp < '2030-01-01' -- remove incorrect dvce_created_tstamps, can cause sql errors
)
select
  coalesce(user_mapping.user_id, email_mapping.email, e.domain_userid) as blended_user_id,
  user_mapping.user_id as inferred_user_id,
  e.*
from events e
left join {{ref('product_identifies')}} user_mapping on user_mapping.domain_userid = e.domain_userid
left join {{ref('email_identifies')}} email_mapping on email_mapping.domain_userid = e.domain_userid
