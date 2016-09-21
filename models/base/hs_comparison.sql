with sources as (
  select
  hs__original__source__type___c as raw_channel,
  hs__original__source__data___1____c f1,
  hs__original__source__data___2____c f2,
  account_id
  from salesforce._contact
  where created_date > '2016-07-04'
),

d1 as (
  select
  account_id,
  case
    when raw_channel = 'DIRECT_TRAFFIC' then 'direct'
    when raw_channel = 'OFFLINE' then 'offline'
    when raw_channel = 'ORGANIC_SEARCH' then 'organic'
    when raw_channel = 'REFERRALS' then 'referral'
    when raw_channel = 'PAID_SEARCH' then 'advertising'
    when raw_channel = 'OTHER_CAMPAIGNS' then 'referral'
    when raw_channel is null then '(none)'
    when raw_channel = 'SOCIAL_MEDIA' then 'social'
    when raw_channel = 'EMAIL_MARKETING' then 'email'
  end as channel
  from sources
),

d2 as (
  select channel, _account.team__id___c as team_id
  from d1
  join salesforce._account on _account.id = account_id
)
select channel, team_id from d2
