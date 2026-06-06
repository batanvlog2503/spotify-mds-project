SELECT
    user_id,
    device_type,
    country,
    count (CASE WHEN event_type = 'play' THEN 1 END) AS plays,
    -- Tổng số bài hát người đó nghe trong ngày
    count (CASE WHEN event_type = 'skip' THEN 1 END) AS skips,
    -- Tổng số lần họ bỏ qua
    count (CASE WHEN event_type = 'add_to_playlist' THEN 1 END) AS playlist_adds,
    -- Tổng số lần họ lưu bài hát vào danh sách yêu thích
    DATE_TRUNC('day', event_ts) AS day
    -- DATE_TRUNC để chặt cái datetime 2026-05-27T15:30:00Z -> 2026-05-27 00:00:00
FROM {{ref('spotify_silver')}}
GROUP BY user_id, device_type, country, DATE_TRUNC('day', event_ts)
ORDER BY plays DESC
