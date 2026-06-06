import os
import json # chuyển đổi dữ liệu từ dạng dictionary thành json
import time # dùng tạo độ treex
import uuid # dùng để tạo id duy nhât cho các lượt nghe
import random # chọn ngẫu nhiêu 1 bài hát hoặc từ 1 nghệ sĩ có trong danh sách

from faker import Faker # chuyên gia tạo dữ liệu giả
from datetime import datetime # ghi lại thời điểm
from kafka import KafkaProducer
# cầu nói để python có thể nói chuyện với gửi dữ lệu tới máy chủ Apache Kafka
from dotenv import load_dotenv
# bảo ật thông tin


#----------------------
# Load Environment variables
#---------------------

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
#địa chỉ server kafka
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "spotify-events")

# tên topic
USER_COUNT = int(os.getenv("USER_COUNT", 20))
EVENT_INTERVAL_SECONDS = int(os.getenv("EVENT_INTERVAL_SECONDS", 1))


fake = Faker() # TAO DỮ LIỆU GIẢ NHƯ USERNAME, EMAIL

# Kafka Producer

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    api_version=(0,11,5),
    value_serializer=lambda v :json.dumps(v).encode("utf-8")
)

# kết nối kafka server và mọi dữ liệu gửi đi sẽ sang json encode sang utf-8
# giả lập hệ thống spotify -> tạo user nghe nhạc -> gửi event vào kafka liên tục
#-------------------------------
# Stable Song / Artist Definitions
# ------------------------------

# tác giả artist và bài hát song
song_artist_pairs = [
    {"artist": "The Weekend", "song": "Blinding Lights"},
    {"artist": "Dua Lipa", "song": "Levitating"},
    {"artist": "Drake", "song": "God's Plan"},
    {"artist": "Taylor Swift", "song": "Love Story"},
    {"artist": "Ed Sheeran", "song": "Shape of You"},
    {"artist": "Kanye West", "song": "Stronger"}

]
# uuid5 cố định phục thuộc vào đầu vào input
# uuid4 là random
for pair in song_artist_pairs:
    name_for_uuid = f"{pair['artist']}::{pair['song']}"
    pair['song_id'] = str(uuid.uuid5(uuid.NAMESPACE_DNS, name_for_uuid))
# hiểu đơn giản cái song_id là id của chỉ 1 bài hát song với 1 artist
devices = ["mobile", "desktop", "web"]
countries = ["US", "UK", "CA", "AU", "IN", "DE"]
event_types = ["play", "pause", "skip", "add_to_playlist"]


# generate random users

user_ids = [str(uuid.uuid4()) for _ in range(USER_COUNT)]
# fake id của 20 user
def generate_event():
    pair = random.choice(song_artist_pairs)
    # chọn 1 bài bất kì
    user_id = random.choice(user_ids)
    # chọn 1 nguiwof nghe bất kì
    return {
        "event_id": str(uuid.uuid4()), # id mooix lần nghe
        "user_id":user_id, # mã người nghe
        "song_id":pair["song_id"], # mã bài hát
        "artist_name":pair["artist"], # ca sĩ
        "song_name":pair["song"], # bài hát
        "event_type": random.choice(event_types), # method
        "device_type":random.choice(devices),# devicde
        "country":random.choice(countries), # country
        "timestamp":datetime.utcnow().isoformat() + "Z"
        # thời gian chuẩn UTC 2026-05-19T10:12:33Z
    }
if __name__ == "__main__":
    print("🎧 Starting Spotify data simulator...")
    print(f"Using {len(song_artist_pairs)} songs and {len(user_ids)} users.")

    for p in song_artist_pairs:
        print(f"{p['song']} — {p['artist']} -> song_id={p['song_id']}")
        # hiện bài hát ca sĩ đi kèm với song_id
    while True: # while true chạy mãi
        event = generate_event()
        # tạo sự kiện
        producer.send(KAFKA_TOPIC, event)
        # gửi event vào topic : spotify-events
        print(f"Produced event: {event['event_type']} - {event['song_name']} by {event['artist_name']} (user {event['user_id']})")
        time.sleep(EVENT_INTERVAL_SECONDS)
        # cách 1 s gửi events