with bronze_data AS (
    SELECT
        event_id,
        user_id,
        song_id,
        artist_name,
        song_name,
        event_type,
        device_type,
        country,
        TRY_TO_TIMESTAMP_TZ(timestamp) AS event_ts -- trả về null nếu timestamp là String != timestamp
    from {{source('bronze', 'spotify_events_bronze')}} -- Giúp dbt tự động tìm đến đúng vị trí file sources.yml bạn đã khai báo để "hút" dữ liệu thô vào xử lý.
    -- sau đó đi vào Schema bronze và table spotify_events_bronze
)

SELECT *
FROM bronze_data
WHERE event_id IS NOT NULL
AND user_id IS NOT NULL
AND song_id IS NOT NULL
AND event_ts IS NOT NULL

-- loại bỏ rác các field có giả trị null
--Dữ liệu sau khi đi qua file spotify_silver này sẽ được dbt tạo thành một Table hoặc View sạch trên Snowflake.