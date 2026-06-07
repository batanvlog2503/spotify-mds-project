-- models/gold/gold_country_stats.sql
select
    country,
    count(case when event_type='play' then 1 end) as total_plays,
    count(distinct user_id)                        as unique_users,
    -- lấy device_type có count cao nhất
    max(device_type) as top_device
from {{ ref('spotify_silver') }}
group by country