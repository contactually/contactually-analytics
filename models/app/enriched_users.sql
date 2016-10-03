with users as (

    select * from {{ ref('users') }}

),

teams as (

    select * from {{ ref('teams') }}

),

payment_accounts as (

    select * from {{ ref('payment_accounts') }}

),

enriched_users_temp as (

    select users.*,
           teams.payment_account_id as team_payment_account_id,
           teams.created_at as team_created_at,
           case when payment_accounts.first_charged_at is null then FALSE
                when users.created_at::date > payment_accounts.first_charged_at::date then TRUE
           else FALSE end as user_added_after_team_paid

    from {{ ref('users') }}

    left outer join teams on teams.id = users.team_id
    left outer join payment_accounts on payment_accounts.id = teams.payment_account_id

),

enriched_users as (

    select *,
    row_number() over (partition by team_id order by user_added_after_team_paid desc, id asc) as user_team_rank
    from enriched_users_temp

)


select *,
case when user_team_rank = 1 then TRUE else FALSE end as is_first_user
from enriched_users
