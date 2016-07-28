select *
from {{ref('all_events')}}
where (se_category != 'identify' or se_category is null)
