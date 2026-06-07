select distinct
    date_trunc('hour', event_ts)          as date_hour,
    to_date(event_ts)                      as date_day,
    extract(hour from event_ts)            as hour,
    extract(dow  from event_ts)            as day_of_week,
    case when extract(dow from event_ts)
         in (0,6) then true else false end as is_weekend
from {{ ref('spotify_silver') }}