
with account as (

    select * from {{ ref('base_account') }}

),

us_and_ca_area_codes as (


    select area_code, state from {{ this.schema }}.area_codes

),

xf as (

    select account.id as account_id,
           billing_street,
           billing_city,
           billing_state,
           billing_postal_code,
           billing_country,
           phone,
           website,
           industry,
           number_of_employees,
           team__id___c as team_id,
           substring(regexp_replace(phone, '[^0-9]', ''), 0, 4) as area_code

           case when billing_country ilike 'us'
                  or billing_country ilike 'usa'
                  or billing_country ilike 'united states'
                  or billing_country ilike 'canada'
                  or billing_country ilike 'ca'
           then FALSE else TRUE end as is_outside_usa_and_canada,

           case when us_and_ca_area_codes.area_code is not null
           then true else false end as phone_has_us_ca_area_code
    from account
    left outer join us_and_ca_area_codes on account.area_code = us_and_ca_area_codes.area_code

)


select
    account_id          as account_account_id,
    billing_street      as account_billing_street,
    billing_city        as account_billing_city,
    billing_state       as account_billing_state,
    billing_postal_code as account_billing_postal_code,
    billing_country     as account_billing_country,
    phone               as account_phone,
    website             as account_website,
    industry            as account_industry,
    number_of_employees as account_number_of_employees,
    team_id             as account_team_id,
    area_cod            as account_area_codee
    is_outside_usa_and_canada as account_is_outside_usa_and_canada,
    phone_has_us_ca_area_code as account_phone_has_us_ca_area_code,
from xf
