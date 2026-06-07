-- models/gold/gold_playlist_conversion.sql
select
    song_id, song_name, artist_name,
    count(case when event_type='play'            then 1 end) as plays,
    count(case when event_type='add_to_playlist' then 1 end) as playlist_adds,
    round(count(case when event_type='add_to_playlist' then 1 end)
        / nullif(count(case when event_type='play' then 1 end),0), 2)
                                                              as conversion_rate
from {{ ref('spotify_silver') }}
group by song_id, song_name, artist_name
order by conversion_rate desc