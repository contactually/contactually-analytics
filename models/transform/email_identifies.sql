--right now, this makes the assumption that your session cookie should be associated with the most recent email address used for it.
--if there have been multiple, the earlier ones are ignored and only the most recent one is used.


with events as (

  select * from {{ref('events')}}

), submits as (

  select * from {{ref('form_submits')}}

), elements as (

  select * from {{ref('form_elements')}}

), logins as (

  select se_label as email, domain_userid
  from events
  where se_category = 'auth' AND
    (se_action = 'signin' or se_action = 'signup')
    and se_label is not null

), resources as (

  select se_property as email, domain_userid
  from events
  where se_category = 'resource'
    and se_property is not null

), hubspot_forms as (

  select elements.value, events.domain_userid
  from submits
    inner join elements on submits.event_id = elements._event_id
    inner join events on submits.event_id = events.event_id
  where elements.value is not null
    and elements.name = 'email'

), combined as (

  select * from logins
  union all
  select * from resources
  union all
  select * from hubspot_forms

), d1 as (

  select
    domain_userid,
    last_value(email) over
      (partition by domain_userid order by collector_tstamp rows between unbounded preceding and unbounded following)
      as email
  from combined

)

select *
from d1
group by 1, 2
