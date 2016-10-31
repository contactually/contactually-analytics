
{% macro channel_mapping_definition(priority, join_pairs, nulls) %}
    select {{ priority }} as priority, s.session_id, c.id as channel_mapping_id
    from normalized_sessions as s
    left outer join channel_mapping as c on
    {% for pair in join_pairs -%} s.{{ pair[0] }} = c.{{ pair[1] }} {% if not loop.last %} and {% endif %} {% endfor -%}
    {% for field in nulls -%} and c.{{field}} is null {% endfor -%}
{% endmacro %}

{% macro clean_utm(utm_val, out_field) %}

  replace(replace(replace(lower(nullif(trim({{ utm_val }}), '')), '%20', ' '), '+', ' '), '%7c', '|') as "{{ out_field }}"

{% endmacro %}



with sessions as (

    select * from {{ ref('snowplow_sessions') }}

), channel_mapping as (

    select * from {{ ref('channel_mapping') }}

), normalized_sessions as (

    select

        sessions.*,
        lower(mkt_medium) as medium,
        lower(mkt_source) as source,
        lower(mkt_campaign) as campaign,
        lower(replace(refr_urlhost, 'www.', '')) as refr_urlhost_clean,
        {{ clean_utm('mkt_medium',   'cleaned_medium') }},
        {{ clean_utm('mkt_source',   'cleaned_source') }},
        {{ clean_utm('mkt_campaign', 'cleaned_campaign') }},
        {{ clean_utm('mkt_content',  'cleaned_content') }},
        {{ clean_utm('mkt_term',     'cleaned_term') }}

    from sessions

),

medium_source_campaign_join as (

    {{ channel_mapping_definition(1, [['medium', 'in_medium'], ['source', 'in_source'], ['campaign', 'in_campaign']], []) }}

),

medium_source_join as (

    {{ channel_mapping_definition(2, [['medium', 'in_medium'], ['source', 'in_source']], ['in_campaign']) }}

),

source_campaign_join as (

    {{ channel_mapping_definition(3, [['source', 'in_source'], ['campaign', 'in_campaign']], ['in_medium'] ) }}

),

medium_join as (

    {{ channel_mapping_definition(4, [['medium', 'in_medium']], ['in_source', 'in_campaign']) }}

),

refr_join as (

    -- This one is custom because we ilike on the referrer. Helpful for google.%, etc
    select
        6 as priority,
        s.session_id,
        c.id as channel_mapping_id
    from normalized_sessions as s
    left outer join channel_mapping as c on s.refr_urlhost_clean ilike c.in_referer

),

all_session_mappings as (

    select * from medium_source_campaign_join
        union all
    select * from medium_source_join
        union all
    select * from source_campaign_join
        union all
    select * from medium_join
        union all
    select * from refr_join

),

best_session_mapping as (


    select session_id, max(channel_mapping_id) as channel_mapping_id from (
        select
            session_id,
            first_value(channel_mapping_id ignore nulls) over (partition by session_id order by priority asc rows unbounded preceding) as channel_mapping_id
        from all_session_mappings
    ) s group by 1

)

{% set is_referral = 'medium is null and source is null and campaign is null and refr_urlhost_clean is not null and out_channel is null' %}

select s.*,
    case when
        medium is null and source is null and campaign is null and refr_urlhost_clean is null then 'direct'
    when {{ is_referral }}
         then 'referral'
    when
        out_channel is null then 'unmapped'
    else
        out_channel
    end as channel,

    -- if it's a referral, make the souce the referring url (if the mapped source is null)
    case when {{ is_referral }} then
        coalesce(out_source, refr_urlhost_clean)
    else
        out_source
    end as mapped_source,

    out_campaign as mapped_campaign
from best_session_mapping as b
    left outer join channel_mapping as c on c.id = b.channel_mapping_id
    inner join normalized_sessions as s on b.session_id = s.session_id
