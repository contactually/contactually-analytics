with product_events as (

  select * from {{ ref('product_events') }}

),

logins as (

    select
      domain_userid,
      domain_sessionidx
    from product_events
    group by 1, 2

)

select domain_userid,
       md5(domain_userid || '|' || domain_sessionidx) as session_id
from logins
