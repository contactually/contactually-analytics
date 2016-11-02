
with users as (

    select * from {{ ref('base_users') }}

)

select
    id,
    team_id,
    created_at,
    updated_at,
    first_name,
    last_name,
    email,
    sign_in_count,
    current_sign_in_at,
    last_sign_in_at,
    last_seen_at,
    admin = 1 as admin,
    dead
from users
--where deleted_at is null
