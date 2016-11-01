
with contact as (

    select * from {{ ref('base_contact') }}

),

us_and_ca_area_codes as (


    select area_code, state from {{ this.schema }}.area_codes

),

xf as (

    select contact.id as contact_id,
           mailing_street,
           mailing_city,
           mailing_state,
           mailing_postal_code,
           mailing_country,
           phone,
           title,
           substring(regexp_replace(phone, '[^0-9]', ''), 0, 4) as area_code,

           case when mailing_country ilike 'us'
                  or mailing_country ilike 'usa'
                  or mailing_country ilike 'united states'
                  or mailing_country ilike 'canada'
                  or mailing_country ilike 'ca'
           then FALSE else TRUE end as is_outside_usa_and_canada,

           case when us_and_ca_area_codes.area_code is not null
           then true else false end as phone_has_us_ca_area_code
    from contact
    left outer join us_and_ca_area_codes on contact.area_code = us_and_ca_area_codes.area_code
)


select contact_id           as  contact_id,
       mailing_street       as  contact_mailing_street,
       mailing_city         as  contact_mailing_city,
       mailing_state        as  contact_mailing_state,
       mailing_postal_code  as  contact_mailing_postal_code,
       mailing_country      as  contact_mailing_country,
       phone                as  contact_phone,
       title                as  contact_title,
       area_code            as  contact_area_code,
       is_outside_usa_and_canada as  contact_is_outside_usa_and_canada,
       phone_has_us_ca_area_code as  contact_phone_has_us_ca_area_code

from xf


