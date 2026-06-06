SELECT
    song_id,
    song_name,
    artist_name,
    COUNT(CASE WHEN event_type = 'play' THEN 1 END) AS total_plays,
    -- hành động người dùng bấm nghe +1
    COUNT(CASE WHEN event_type = 'skip' THEN 1 END) AS total_skips
    -- hành động ngươời dùng skip thì +1
FROM {{ref('spotify_silver')}}
-- kĩ năng của dbt gióng với việc tìm bảng ấy
-- SPOTIFY_DB.TRANSFORM.spotify_silver
-- ref tự động hiểu dữ liệu cần lấy ở đâu

GROUP BY song_id, song_name, artist_name
ORDER BY total_plays DESC
