with events as (

  select * from {{ref('events')}}

)

select *
from events
where (se_category != 'identify' or se_category is null)
