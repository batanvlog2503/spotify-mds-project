import json
import os
from kafka import KafkaConsumer

from datetime import datetime
import boto3
from dotenv import load_dotenv



# ----------------------- load environment variables -----------------------
load_dotenv()

# ---------------------- Configuration -----------------------

MINIO_BUCKET = os.getenv("MINIO_BUCKET")
# cái giỏ chứa dữ liệu MINIO mà bạn muốn ném file vào
# data-lake, customer-logs

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
# địa chỉ nhà của MINIO nó sẽ là http://minio:9000
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
# cái access, secret giống như tk, password để đăng nhập minio

MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
# tên hộp thư mà kafka muốn nhận tin nhắn.
# một kafka có thể có nhiều topic như spotify-events. topic-giao-dich
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVER")
# địa chỉ cổng kết nối kafka
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")
# định danh cho nhóm người đọc, cái này rất quan trọng trong kafka.
# nó giúp đánh dấu xem chương trình kafka đọc đến tin nhắn thứ bao nhiêu rồi
# giả sử chương trình bị tắt đi bật lại, nó sẽ dựa vào GROUP_ID
# để độc tiếp những tin nhắn đó
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))

# hiểu đơn giản khi bạn có 1 tin nhắn từ kafka đến mà lại phải mở MINIO
# thì rất nghẽn và tốn tài nguyên
# 10 nghĩa là khi gom đủ 10 messasge thì kafka đóng gói thành 1 file duy nhất
# rồi mới thực hiện đẩ lên MINIO
# [Kafka Topic] ──► (Hút từng message) ──► [Bộ nhớ đệm Code] (Gom đủ 10 cái)
#                                                  │
#                                                  ▼ (Đóng gói thành file)
# [MinIO Bucket] ◄── (Đẩy cả file lên bằng Boto3) ─┘

# connect to minIO

# --------------connect to MINIO ---------------------
# boto3 tạo ra 1 đói tượng tên là s3
# đối tuoượng này có toàn quyền năng như
# tạo bucket, upload file, download fil
#Tôi muốn dùng giao thức lưu trữ dạng S3 (Simple Storage Service)".

s3 = boto3.client(
    "s3",
    endpoint_url = MINIO_ENDPOINT, # kết
    aws_access_key_id = MINIO_ACCESS_KEY,
    aws_secret_access_key = MINIO_SECRET_KEY
)

# hiểu đơn giản boto3 tạo ra để kết nối với amazon s3 cloud
# nhưng chúng ta fake sử dụng minio thay vi aws

# ensure bucket exists (idemopotent)
# kiểm tra thùng chứa có tồn tại không để
# kafka bắt đầu hút dữ liệu
# trong s3 (như minio hay aws s3) thì s3 là đơn vị lưu trữ cao nhất
try:
    s3.head_bucket(Bucket=MINIO_BUCKET)
    print(f"Bucket {MINIO_BUCKET} already exists.")
except Exception:
    s3.create_bucket(Bucket=MINIO_BUCKET)
    print(f"Created bucket {MINIO_BUCKET}.")

# -----------------------KAFKA consumer setup

#consumer : người tiêu thụ
#bắt đầu hút dữ liệu

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    # máy chỉ hút dữ liệu trong topic nay
    bootstrap_servers = [KAFKA_BOOTSTRAP_SERVERS],
    # thết lập liên kết địa chỉ kafka
    auto_offset_reset = "earliest",
    # offset là stt của mỗi tin nhắn
    # nếu mất kết nối thì earliest đọc lại
    # nếu mấ kêt nối thì latest: bỏ qua quá khứ đọc lại từ lúc này

    enable_auto_commit=True,
# Khi chương trình hút dữ liệu về, nó cần báo lại với Kafka rằng:
    # "Tôi đã đọc xong tin nhắn này rồi nhé". Hành động báo cáo này gọi là Commit.
    group_id = KAFKA_GROUP_ID,
    # dánh dấu ID đã đọc đến sau ví dụ mất kêt nối
    # còn tìm thấy được
    value_deserializer = lambda v : json.loads(v.decode("utf-8"))
    # dữ liệu đi vào kafka bin thành nhị phân
    # ta encode thành utf-8 và chuyển thành json
)


print(f"🎧 Listening for events on Kafka topic '{KAFKA_TOPIC}'...")

#đanh sách dợi đủ 10 tin nhắn ấy
batch =[]

for message in consumer:
    event = message.value
    # lấy ra dữ liêu bên trong topic
    batch.append(event)
    # thêm vào batch
    if len(batch) >= BATCH_SIZE:
        now = datetime.utcnow()
        date_path = now.strftime("date=%Y-%m-%d/hour=%H")

        # kiểu: date = 2026 - 05 - 19 / hour = 23.
        file_name = f"spotify_events_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
        #spotify_events_2026-05-19T23-00-12.json.
        file_path = f"bronze/{date_path}/{file_name}"

        json_data = "\n".join([json.dumps(e) for e in batch])
        # 👉 bronze / date = 2026 - 05 - 19 / hour = 23 / spotify_events_2026 - 05 - 19
        # T23 - 00 - 12.j
        # son
        s3.put_object(

            Bucket = MINIO_BUCKET,
            Key = file_path,
            Body = json_data.encode("utf-8")
        )
        # put lên MINIO
        # đóng gói dữ liệu thành định danjg json line
        print(f"✅ Uploaded {len(batch)} events to MinIO: {file_path}")
        batch = []

