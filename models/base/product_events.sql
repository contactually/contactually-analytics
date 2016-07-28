select *
from {{ref('all_events')}}
where se_category = 'identify'
