with base as (

    select coalesce(
        object_story_spec__link_data__call_to_action__value__link,
        object_story_spec__link_data__link,
        link_url
    ) as url,
    *
    from
      facebook_contactually_ads.facebook_adcreative_26288427
)

select
  id,
  url_tags,
  url,
  split_part(url ,'?',1) as base_url,
  split_part(split_part(url_tags,'utm_source=',2), '&', 1) as utm_source,
  split_part(split_part(url_tags,'utm_medium=',2), '&', 1) as utm_medium,
  split_part(split_part(url_tags,'utm_campaign=',2), '&', 1) as utm_campaign,
  split_part(split_part(url_tags,'utm_content=',2), '&', 1) as utm_content,
  split_part(split_part(url_tags,'utm_term=',2), '&', 1) as utm_term
from base
