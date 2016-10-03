
with contact as (

    select * from {{ ref('base_contact') }}

)

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
       contact.account_id

from contact
