select distinct
    song_id, song_name, artist_name,
    genre, duration_seconds
from {{ ref('spotify_silver') }}