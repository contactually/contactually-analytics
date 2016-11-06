with events as (

    select * from {{ ref('base_events') }}

)

select *
from events
where se_category = 'identify'
