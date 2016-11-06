
{% set fields = ['in_medium', 'in_source', 'in_campaign', 'in_referer', 'out_channel', 'out_source', 'out_campaign'] %}

with channel_mapping as (

    select * from fivetran_uploads.channel_mapping

)


select

    {% for field in fields -%}
        lower(nullif(trim({{ field }}), '')) as {{ field }},
    {% endfor -%}
    md5({% for field in fields -%} coalesce({{ field }}, 'NULL') {% if not loop.last %} || '-' || {%endif%} {% endfor -%}) as id

from channel_mapping

