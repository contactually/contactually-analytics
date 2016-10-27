
with account as (

    select * from {{ ref('base_account') }}

),

base as (

    select account.id,
           account.name,
           account.billing_street,
           account.billing_city,
           account.billing_state,
           account.billing_postal_code,
           account.billing_country,
           account.phone,
           account.website,
           account.industry,
           account.number_of_employees,
           account.team__id___c as team_id,
           substring(regexp_replace(phone, '[^0-9]', ''), 0, 4) as area_code
    from account

),

us_and_ca_area_codes as (


    select area_code, state from {{ this.schema }}.area_codes

)

select base.*,
       case when billing_country ilike 'us'
              or billing_country ilike 'usa'
              or billing_country ilike 'united states'
              or billing_country ilike 'canada'
              or billing_country ilike 'ca'
       then FALSE else TRUE end as is_outside_usa_and_canada,

       case when us_and_ca_area_codes.area_code is not null
       then true else false end as phone_has_us_ca_area_code


from base
left outer join us_and_ca_area_codes on base.area_code = us_and_ca_area_codes.area_code
