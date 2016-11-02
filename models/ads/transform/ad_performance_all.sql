

{{ config(
    materialized='table',
    sort=['id'],
    dist='service'
  )
}}

{% macro clean_utm(utm_val, out_field) %}

  replace(replace(replace(lower(nullif(trim({{ utm_val }}), '')), '%20', ' '), '+', ' '), '%7c', '|') as "{{ out_field }}"

{% endmacro %}

with adwords as (

    select * from {{ ref('adwords_performance_stitch') }}
),

fb_ads as (

    select * from {{ ref('facebook_performance_stitch') }}
),

unioned as (

    select 'adwords' as service, * from adwords
    union all
    select 'fb-ads' as service, * from fb_ads

),

cleaned as (
    select
        id,
        service,
        date,
        campaign_id,
        url,
        replace(replace(lower(nullif(trim(base_url), '')), 'http://', ''), 'https://', '') as base_url,
        impressions,
        clicks,
        cost,

        {{ clean_utm('utm_medium', 'utm_medium') }},
        {{ clean_utm('utm_source', 'utm_source') }},
        {{ clean_utm('utm_campaign', 'utm_campaign') }},
        {{ clean_utm('utm_content', 'utm_content') }},
        {{ clean_utm('utm_term', 'utm_term') }}
    from unioned
)

select
    md5(
        service
        || '-' || date
        || '-' || campaign_id
        || '-' || url
        || '-' || base_url
        || '-' || coalesce(utm_medium, '')
        || '-' || coalesce(utm_source, '')
        || '-' || coalesce(utm_campaign, '')
        || '-' || coalesce(utm_content, '')
        || '-' || coalesce(utm_term, '')
    ) as id,
    service,
    date,
    campaign_id,
    url,
    base_url,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_content,
    utm_term,
    sum(impressions) as impressions,
    sum(clicks)      as click,
    sum(cost)        as cost
from cleaned
where "date" > '2016-07-04'
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
