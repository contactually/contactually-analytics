with adwords as
(
  select
    *
  from {{ ref('adwords_ads_base') }}
), facebook as
(
  select
    *
  from {{ ref('facebook_ads_base') }}
)
select
  *
from adwords
union all
select
  *
from facebook