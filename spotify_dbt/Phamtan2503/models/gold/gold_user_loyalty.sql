-- models/gold/gold_user_loyalty.sql
select
    user_id,
    count(distinct to_date(event_ts)) as active_days,
    case
        when count(distinct to_date(event_ts)) >= 5 then 'Power'
        when count(distinct to_date(event_ts)) >= 3 then 'Regular'
        else 'Casual'
    end as loyalty_tier
from {{ ref('spotify_silver') }}
group by user_id