
{{

    config(enabled=false)

}}

with users as (

    select * from {{ ref('users') }}

),

teams as (

    select * from {{ ref('teams') }}

),

payment_accounts as (

    select * from {{ ref('payment_accounts') }}

),

team_all_users as (

    select users.id, users.team_id,
           case when payment_accounts.first_charged_at is null then FALSE
                when users.created_at::date > payment_accounts.first_charged_at::date then TRUE
           else FALSE end as user_added_after_team_paid

    from {{ ref('users') }}

    inner join teams on teams.id = users.team_id
    left outer join payment_accounts on payment_accounts.id = teams.payment_account_id

),

team_first_user as (

    select * from (
        select team_id, user_id,
        row_number() over (partition by team_id, order by user_id) as row_number
        from team_all_users where user_added_after_team_paid = FALSE
        group by 1, 2
    ) where row_number = 1
)

select 
       teams.id as team_id,
       teams.domain as team_domain,
       teams.name as team_name,
       teams.created_at as team_created_at,

       payment_accounts.plan as team_payment_plan,
       payment_accounts.id as payment_account_id,

       users.id,
       users.first_name,
       users.last_name,
       users.created_at,
       users.email,
       users.sign_in_count,
       users.last_sign_in_at,

       (users.created_at::date > payment_accounts.first_charged_at::date or payment_accounts.first_charged_at is null) as user_added_after_team_paid

from {{ ref('teams') }} teams on teams.id = users.team_id
left outer join {{ ref('users') }} users
left outer join {{ ref('payment_accounts') }} payment_accounts on payment_accounts.id = teams.payment_account_id
