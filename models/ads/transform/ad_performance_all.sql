

{{ config(
    materialized='table',
    sort=['id'],
    dist='service'
  )
}}

{% macro channel_mapping_definition(priority, join_pairs, nulls) %}
    select {{ priority }} as priority, a.id, c.id as channel_mapping_id
    from normalized_ads as a
    left outer join channel_mapping as c on
    {% for pair in join_pairs -%} a.{{ pair[0] }} = c.{{ pair[1] }} {% if not loop.last %} and {% endif %} {% endfor -%}
    {% for field in nulls -%} and c.{{field}} is null {% endfor -%}
{% endmacro %}

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

), channel_mapping as (

    select * from {{ ref('channel_mapping') }}

),

normalized_ads as (
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
),


medium_source_campaign_join as (

    {{ channel_mapping_definition(1, [['utm_medium', 'in_medium'], ['utm_source', 'in_source'], ['utm_campaign', 'in_campaign']], []) }}

),

medium_source_join as (

    {{ channel_mapping_definition(2, [['utm_medium', 'in_medium'], ['utm_source', 'in_source']], ['in_campaign']) }}

),

source_campaign_join as (

    {{ channel_mapping_definition(3, [['utm_source', 'in_source'], ['utm_campaign', 'in_campaign']], ['in_medium'] ) }}

),

medium_join as (

    {{ channel_mapping_definition(4, [['utm_medium', 'in_medium']], ['in_source', 'in_campaign']) }}

),

all_ad_mappings as (

    select * from medium_source_campaign_join
        union all
    select * from medium_source_join
        union all
    select * from source_campaign_join
        union all
    select * from medium_join

),

best_ad_mapping as (


    select id, max(channel_mapping_id) as channel_mapping_id from (
        select
            id,
            first_value(channel_mapping_id ignore nulls) over (partition by id order by priority asc rows unbounded preceding) as channel_mapping_id
        from all_ad_mappings
    ) a group by 1

), mapped as (

    select a.*,
        coalesce(out_source, in_source, utm_source) as mapped_source,
        coalesce(out_campaign, in_campaign, utm_campaign) as mapped_campaign,

        -- for deduplication (if channel mapping applies to > 1 session)
        row_number() over (partition by a.id order by c.id) as dedupe

    from best_ad_mapping as b
        left outer join channel_mapping as c on c.id = b.channel_mapping_id
        inner join normalized_ads as a on b.id = a.id
)

select
    md5(
        service
        || '-' || date
        || '-' || coalesce(campaign_id, '')
        || '-' || coalesce(url, '')
        || '-' || coalesce(base_url, '')
        || '-' || coalesce(utm_medium, '')
        || '-' || coalesce(mapped_source, '')
        || '-' || coalesce(mapped_campaign, '')
        || '-' || coalesce(utm_content, '')
        || '-' || coalesce(utm_term, '')
    ) as id,
    service,
    date,
    campaign_id,
    url,
    base_url,
    utm_medium,
    mapped_source as utm_source,
    mapped_campaign as utm_campaign,
    utm_content,
    utm_term,

    -- for joining to sessions
    utm_source as og_utm_source,
    utm_campaign as og_utm_campaign,

    sum(impressions) as impressions,
    sum(clicks)      as clicks,
    sum(cost)        as cost
from mapped
where "date" > '2016-07-04'
and dedupe = 1
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
