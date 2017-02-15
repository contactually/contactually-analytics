

select
    creative.*,
    creative_child.call_to_action__value__link as link_url

from facebook_contactually_ads.facebook_adcreative_26288427 as creative
left outer join facebook_contactually_ads.facebook_adcreative_26288427__object_story_spec__link_data__child_attachments as creative_child
    on creative_child._sdc_source_key_id = creative.id
