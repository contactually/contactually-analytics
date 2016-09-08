select
  id,
  url_tags,
  split_part(object_story_spec__link_data__link ,'?',1) as base_url,
  object_story_spec__link_data__link as url,
  split_part(split_part(url_tags,'utm_source=',2), '&', 1) as utm_source,
  split_part(split_part(url_tags,'utm_medium=',2), '&', 1) as utm_medium,
  split_part(split_part(url_tags,'utm_campaign=',2), '&', 1) as utm_campaign,
  split_part(split_part(url_tags,'utm_content=',2), '&', 1) as utm_content,
  split_part(split_part(url_tags,'utm_term=',2), '&', 1) as utm_term
from
  facebook_contactually_ads.facebook_adcreative_26288427
