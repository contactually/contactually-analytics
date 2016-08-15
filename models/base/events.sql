
with events as (
  select *,
    rank() over(partition by domain_userid, domain_sessionidx order by dvce_tstamp) as rank,
    count(*) over (partition by domain_userid, domain_sessionidx) as total
   from {{ref('all_events')}}
   where (se_category != 'identify' or se_category is null)
),
ranked as (
  select *,
   lag(page_url) over(partition by domain_userid order by domain_sessionidx, rank) as prev_page,
   lag(domain_sessionidx) over(partition by domain_userid order by domain_sessionidx, rank) as prev_idx,
   lag(domain_sessionid) over(partition by domain_userid order by domain_sessionidx, rank) as prev_id,
   lag(is_last) over (partition by domain_userid order by domain_sessionidx, rank) as prev_was_last
   from (
     select *,
     case when rank = 1 then true else false end as is_first,
     case when rank = total then true else false end as is_last
     from events
   )
   where is_first = true or is_last = true
),
with_pseudo_session_id as (
  select *,
  case when prev_page = page_url and prev_idx + 1 = domain_sessionidx and is_first and prev_was_last and event = 'pp' then
     prev_idx
  else
     domain_sessionidx
  end as pseudo_sessionidx,
  case when prev_page = page_url and prev_idx + 1 = domain_sessionidx and is_first and prev_was_last and event = 'pp' then
     prev_id
  else
     domain_sessionid
  end as pseudo_sessionid
  from ranked
)

select
    app_id,
    br_colordepth,
    br_cookies,
    br_family,
    br_features_director,
    br_features_flash,
    br_features_gears,
    br_features_java,
    br_features_pdf,
    br_features_quicktime,
    br_features_realplayer,
    br_features_silverlight,
    br_features_windowsmedia,
    br_lang,
    br_name,
    br_renderengine,
    br_type,
    br_version,
    br_viewheight,
    collector_tstamp,
    doc_charset,
    doc_height,
    doc_width,
    domain_sessionid as original_sessionid,
    domain_sessionidx as original_sessionidx,
    pseudo_sessionid as domain_sessionid,
    pseudo_sessionidx as domain_sessionidx,
    domain_userid,
    dvce_ismobile,
    dvce_screenheight,
    dvce_sent_tstamp,
    dvce_tstamp,
    dvce_type,
    event,
    event_format,
    event_id,
    event_name,
    event_vendor,
    event_version,
    mkt_campaign,
    mkt_content,
    mkt_medium,
    mkt_source,
    mkt_term,
    name_tracker,
    network_userid,
    os_family,
    os_manufacturer,
    os_name,
    os_timezone,
    page_referrer,
    page_title,
    page_url,
    page_urlfragment,
    page_urlhost,
    page_urlpath,
    page_urlport,
    page_urlquery,
    page_urlscheme,
    platform,
    pp_xoffset_max,
    pp_xoffset_min,
    pp_yoffset_max,
    pp_yoffset_min,
    refr_medium,
    refr_source,
    refr_term,
    refr_urlfragment,
    refr_urlhost,
    refr_urlpath,
    refr_urlport,
    refr_urlquery,
    refr_urlscheme,
    se_action,
    se_category,
    se_label,
    se_property,
    se_value,
    sequence_number,
    ti_category,
    ti_currency,
    ti_name,
    ti_orderid,
    ti_price,
    ti_quantity,
    ti_sku,
    tr_affiliation,
    tr_city,
    tr_country,
    tr_currency,
    tr_orderid,
    tr_shipping,
    tr_state,
    tr_tax,
    tr_total,
    txn_id,
    user_fingerprint,
    user_id,
    user_ipaddress,
    useragent,
    v_collector,
    v_tracker
from with_pseudo_session_id
