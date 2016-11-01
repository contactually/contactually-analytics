
{{
    config (
        materialized ='table',
        sort = ['team_created_at'],
        dist = 'team_id'
    )
}}

with teams as (

    select * from {{ ref('app_teams') }}

),

accounts as (

    select * from {{ ref('sf_accounts') }}

),

payment_accounts as (

    select * from {{ ref('app_payment_accounts') }}

)

select
    -- id fields
    teams.id            as team_id,
    accounts.account_id as account_id,

    -- team fields
    --teams.domain      as team_domain, -- this is always null?
    teams.created_at  as team_created_at,
    teams.updated_at  as team_updated_at,
    teams.name        as team_name,
    teams.trial_days  as team_trial_days,

    -- payment account fields
    payment_accounts.plan              as payment_account_plan,
    payment_accounts.coupon            as payment_account_coupon,
    payment_accounts.quantity          as payment_account_quantity,
    payment_accounts.created_at        as payment_account_created_at,
    payment_accounts.updated_at        as payment_account_updated_at,
    payment_accounts.first_charged_at  as payment_account_first_charged_at,
    payment_accounts.last_charged_at   as payment_account_last_charged_at,
    payment_accounts.partner_name      as payment_account_partner_name,

    -- account fields
    account_billing_street,
    account_billing_city,
    account_billing_state,
    account_billing_postal_code,
    account_billing_country,
    account_phone,
    account_website,
    account_industry,
    account_number_of_employees,
    account_area_code,
    account_is_outside_usa_and_canada,
    account_phone_has_us_ca_area_code


from teams
    join accounts on teams.id = accounts.account_team_id
    left outer join payment_accounts on payment_accounts.id = teams.payment_account_id

