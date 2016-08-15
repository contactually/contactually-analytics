select channel, source, campaign, sum(score) as score
from {{ref("multitouch_timeseries")}}
group by 1, 2, 3
