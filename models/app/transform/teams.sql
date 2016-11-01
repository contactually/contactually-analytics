
with teams as (

    select * from {{ ref('base_teams') }}

),


select id,
       domain,
       created_at,
       updated_at,
       name,
       payment_account_id,
       trial_days
from teams
