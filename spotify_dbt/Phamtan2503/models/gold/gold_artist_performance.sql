-- models/gold/gold_artist_performance.sql
select
    artist_name,
    count(case when event_type='play' then 1 end)  as total_plays,
    count(case when event_type='skip' then 1 end)  as total_skips,
    round(count(case when event_type='skip' then 1 end)
        / nullif(count(case when event_type='play' then 1 end),0), 2)
                                                    as skip_rate,
    count(distinct user_id)                         as unique_listeners
from {{ ref('spotify_silver') }}
group by artist_name