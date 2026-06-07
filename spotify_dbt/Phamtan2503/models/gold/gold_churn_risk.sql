-- models/gold/gold_churn_risk.sql
select
    user_id,
    max(to_date(event_ts))  as last_active_date,
    datediff(day, max(to_date(event_ts)), current_date()) as days_inactive,
    round(count(case when event_type='skip' then 1 end)
        / nullif(count(*), 0), 2)                  as skip_rate,
    case
        when datediff(day, max(to_date(event_ts)),
             current_date()) > 7  then 'high'
        when skip_rate > 0.7      then 'medium'
        else 'low'
    end as churn_risk
from {{ ref('spotify_silver') }}
group by user_id