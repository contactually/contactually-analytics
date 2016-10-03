
with account as (

    select * from {{ ref('base_account') }}

)

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
       account.team__id___c as team_id

from account
