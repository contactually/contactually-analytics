
with contact as (

    select * from {{ ref('base_contact') }}

),

base as (

    select contact.first_name,
           contact.last_name,
           contact.name,
           contact.mailing_street,
           contact.mailing_city,
           contact.mailing_state,
           contact.mailing_postal_code,
           contact.mailing_country,
           contact.phone,
           contact.email,
           contact.title,
           contact.account_id,
           substring(regexp_replace(phone, '[^0-9]', ''), 0, 4) as area_code
    from contact
),

us_and_ca_area_codes as (


    select area_code, state from {{ this.schema }}.area_codes

)

select
       base.*,
       case when mailing_country ilike 'us'
              or mailing_country ilike 'usa'
              or mailing_country ilike 'united states'
              or mailing_country ilike 'canada'
              or mailing_country ilike 'ca'
       then FALSE else TRUE end as is_outside_usa_and_canada,

       case when us_and_ca_area_codes.area_code is not null
       then true else false end as phone_has_us_ca_area_code

from base
left outer join us_and_ca_area_codes on base.area_code = us_and_ca_area_codes.area_code
