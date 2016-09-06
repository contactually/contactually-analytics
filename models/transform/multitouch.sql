select channel, source, campaign, sum(attribution_points) as score
from {{ref("multitouch_timeseries")}}
group by 1, 2, 3
