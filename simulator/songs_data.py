# songs_data.py
# Chứa toàn bộ dữ liệu bài hát và nghệ sĩ
# Tách riêng để dễ thêm bài mới mà không sửa producer.py

import uuid

def build_song_artist_pairs():
    """
    Trả về list song_artist_pairs với song_id đã được tính sẵn
    uuid5 cố định — cùng tên bài + nghệ sĩ → luôn cùng song_id
    """
    raw_pairs = [
        {
            "artist":           "The Weeknd",
            "song":             "Blinding Lights",
            "genre":            "Pop",
            "duration_seconds": 200,
            "release_year":     2019,
            "album_name":       "After Hours"
        },
        {
            "artist":           "Dua Lipa",
            "song":             "Levitating",
            "genre":            "Pop",
            "duration_seconds": 203,
            "release_year":     2020,
            "album_name":       "Future Nostalgia"
        },
        {
            "artist":           "Drake",
            "song":             "God's Plan",
            "genre":            "Hip-Hop",
            "duration_seconds": 198,
            "release_year":     2018,
            "album_name":       "Scorpion"
        },
        {
            "artist":           "Taylor Swift",
            "song":             "Love Story",
            "genre":            "Country Pop",
            "duration_seconds": 235,
            "release_year":     2008,
            "album_name":       "Fearless"
        },
        {
            "artist":           "Ed Sheeran",
            "song":             "Shape of You",
            "genre":            "Pop",
            "duration_seconds": 234,
            "release_year":     2017,
            "album_name":       "Divide"
        },
        {
            "artist":           "Kanye West",
            "song":             "Stronger",
            "genre":            "Hip-Hop",
            "duration_seconds": 311,
            "release_year":     2007,
            "album_name":       "Graduation"
        },
        {
            "artist":           "Billie Eilish",
            "song":             "Bad Guy",
            "genre":            "Electropop",
            "duration_seconds": 194,
            "release_year":     2019,
            "album_name":       "When We All Fall Asleep"
        },
        {
            "artist":           "BTS",
            "song":             "Dynamite",
            "genre":            "K-Pop",
            "duration_seconds": 199,
            "release_year":     2020,
            "album_name":       "BE"
        },
    ]

    # Gắn song_id cố định cho từng bài
    for pair in raw_pairs:
        name_for_uuid = f"{pair['artist']}::{pair['song']}"
        pair["song_id"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, name_for_uuid))

    return raw_pairs