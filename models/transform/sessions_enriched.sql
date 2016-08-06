with sessions as (

  select * from {{ref('sessions')}}

), email_identifies as (

  select * from {{ref('email_identifies')}}

), product_identifies as (

  select * from {{ref('product_identifies')}}

), product_logins as (

  select * from {{ref('product_logins')}}

), session_channels as (

  select * from {{ref('campaign_channels')}}

)

select
  sessions.domain_userid || '-' || sessions.domain_sessionidx as session_id,
  sessions.*,
  emails.email,
  userids.user_id,
  case
    when logins.domain_userid is not null then true
    else false
  end as user_logged_in,
  channels.channel,
  channels.enriched_medium,
  channels.enriched_source
from sessions
  left join email_identifies as emails
    on  sessions.domain_userid = emails.domain_userid
  left join product_identifies as userids
    on  sessions.domain_userid = userids.domain_userid
  left join product_logins as logins
    on  sessions.domain_userid = logins.domain_userid
    and sessions.domain_sessionidx = logins.domain_sessionidx
  left join session_channels as channels
    on  (sessions.domain_userid || '-' || sessions.domain_sessionidx) = channels.session_id
where (emails.email not ilike '%contactually.com' or emails.email is null)
