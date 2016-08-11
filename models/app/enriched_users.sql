select users.*,
       teams.name as team_name,
       (users.created_at::date > payment_accounts.first_charged_at::date or
        payment_accounts.first_charged_at is null) as user_added_after_team_paid
from {{ ref('users') }} users
join {{ ref('teams') }} teams on teams.id = users.team_id
join {{ ref('payment_accounts') }} payment_accounts on payment_accounts.id = teams.payment_account_id
