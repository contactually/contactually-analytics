select users.id,
       users.first_name,
       users.last_name,
       users.created_at,
       users.email,
       users.sign_in_count,
       users.last_sign_in_at,
       teams.domain as team_domain,
       teams.name as team_name,
       teams.created_at as team_created_at,
       payment_accounts.plan as team_payment_plan,
       (users.created_at::date > payment_accounts.first_charged_at::date or
        payment_accounts.first_charged_at is null) as user_added_after_team_paid
from {{ ref('users') }} users
left outer join {{ ref('teams') }} teams on teams.id = users.team_id
left outer join {{ ref('payment_accounts') }} payment_accounts on payment_accounts.id = teams.payment_account_id
