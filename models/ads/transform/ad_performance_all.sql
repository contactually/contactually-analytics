

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

select * from cleaned
where "date" > '2016-07-04'
