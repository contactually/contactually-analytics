
with users as (

    select * from {{ ref('base_users') }}

)

select *
from users
where deleted_at is null
