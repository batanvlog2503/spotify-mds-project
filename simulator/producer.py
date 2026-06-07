import os
import json # chuyển đổi dữ liệu từ dạng dictionary thành json
import time # dùng tạo độ trễ
import uuid # dùng để tạo id duy nhất cho các lượt nghe
import random # chọn ngẫu nhiên 1 bài hát hoặc từ 1 nghệ sĩ có trong danh sách

from faker import Faker # chuyên gia tạo dữ liệu giả
from datetime import datetime # ghi lại thời điểm
from kafka import KafkaProducer
from dotenv import load_dotenv
from songs_data import build_song_artist_pairs
# ----------------------
# Load Environment variables
# ---------------------

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "spotify-events")
USER_COUNT = int(os.getenv("USER_COUNT", 20))
EVENT_INTERVAL_SECONDS = int(os.getenv("EVENT_INTERVAL_SECONDS", 1))

fake = Faker()

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    api_version=(0, 11, 5),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ------------------------------
# Stable Song / Artist Definitions
# ------------------------------

# ✅ THÊM MỚI: genre, duration_seconds, release_year, album_name
# uuid5 cố định phụ thuộc vào đầu vào input
song_artist_pairs = build_song_artist_pairs()
for pair in song_artist_pairs:
    name_for_uuid = f"{pair['artist']}::{pair['song']}"
    pair['song_id'] = str(uuid.uuid5(uuid.NAMESPACE_DNS, name_for_uuid))

devices = ["mobile", "desktop", "web"]
countries = ["US", "UK", "CA", "AU", "IN", "DE", "VN", "JP"]
event_types = ["play", "pause", "skip", "add_to_playlist"]

# ✅ THÊM MỚI: subscription_type gắn cố định với mỗi user
# để đảm bảo 1 user không lúc free lúc premium
user_profiles = [
    {
        "user_id": str(uuid.uuid4()),
        "username": fake.user_name(),
        "age": random.randint(16, 45),
        "subscription_type": random.choice(["free", "free", "premium"]),
        # tỉ lệ 2:1 free:premium — sát thực tế hơn
        "registration_date": fake.date_between(
            start_date="-3y", end_date="today"
        ).isoformat(),
    }
    for _ in range(USER_COUNT)
]

def generate_event():
    pair = random.choice(song_artist_pairs)
    # chọn 1 bài bất kì

    user = random.choice(user_profiles)
    # chọn 1 người nghe bất kì — giờ mang theo đầy đủ profile

    return {
        # --- fields cũ ---
        "event_id":         str(uuid.uuid4()),      # id mỗi lần nghe
        "user_id":          user["user_id"],         # mã người nghe
        "song_id":          pair["song_id"],         # mã bài hát
        "artist_name":      pair["artist"],          # ca sĩ
        "song_name":        pair["song"],            # bài hát
        "event_type":       random.choice(event_types),
        "device_type":      random.choice(devices),
        "country":          random.choice(countries),
        "timestamp":        datetime.utcnow().isoformat() + "Z",

        # --- ✅ fields mới: user info ---
        "username":         user["username"],
        "age":              user["age"],
        "subscription_type": user["subscription_type"],
        "registration_date": user["registration_date"],

        # --- ✅ fields mới: song info ---
        "genre":            pair["genre"],
        "duration_seconds": pair["duration_seconds"],
        "release_year":     pair["release_year"],
        "album_name":       pair["album_name"],
    }

if __name__ == "__main__":
    print("🎧 Starting Spotify data simulator...")
    print(f"Using {len(song_artist_pairs)} songs and {len(user_profiles)} users.")
    print()

    print("📀 Songs:")
    for p in song_artist_pairs:
        print(f"  {p['song']} — {p['artist']} ({p['genre']}, {p['release_year']}) -> song_id={p['song_id']}")

    print()
    print("👤 Sample users:")
    for u in user_profiles[:3]:  # chỉ in 3 user đầu để không spam terminal
        print(f"  {u['username']} | age={u['age']} | {u['subscription_type']} | registered={u['registration_date']}")
    print(f"  ... and {len(user_profiles) - 3} more users")
    print()

    while True:
        event = generate_event()
        producer.send(KAFKA_TOPIC, event)
        print(
            f"[{event['timestamp']}] {event['event_type'].upper():18s} "
            f"| {event['song_name']:20s} | {event['artist_name']:15s} "
            f"| {event['subscription_type']:7s} | {event['country']}"
        )
        time.sleep(EVENT_INTERVAL_SECONDS)

# thêm field
# Song pairs — thêm 4 field cho mỗi bài: genre, duration_seconds, release_year, album_name. Thêm 2 bài mới (Billie Eilish, BTS) để đa dạng genre hơn.
# User profiles — thay user_ids (chỉ là list UUID) bằng user_profiles (list dict đầy đủ).
# Mỗi user giờ có username, age, subscription_type, registration_date cố định — đảm bảo 1 user không lúc free lúc premium.
