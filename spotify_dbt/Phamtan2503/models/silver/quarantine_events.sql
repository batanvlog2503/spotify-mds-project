-- models/silver/quarantine_events.sql
with raw as (
    select * from {{ source('bronze', 'spotify_events_bronze') }}
)
select *,
    case
        when event_id  is null then 'missing event_id'
        when user_id   is null then 'missing user_id'
        when song_id   is null then 'missing song_id'
        when timestamp is null then 'missing timestamp'
        else 'invalid timestamp'
    end as reject_reason,
    current_timestamp() as rejected_at
from raw
where event_id  is null
   or user_id   is null
   or song_id   is null
   or TRY_TO_TIMESTAMP_TZ(timestamp) is null