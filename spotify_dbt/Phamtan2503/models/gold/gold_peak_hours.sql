-- models/gold/gold_peak_hours.sql
select
    country,
    extract(hour from event_ts) as hour,
    count(*)                    as total_events
from {{ ref('spotify_silver') }}
where event_type = 'play'
group by country, extract(hour from event_ts)
order by country, total_events desc
