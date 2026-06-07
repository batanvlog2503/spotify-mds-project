import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp,
    when, lit
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)

# ✅ Thêm vào PHẦN 1, sau load_dotenv

os.environ["JAVA_HOME"]   = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.31.11-hotspot"
os.environ["HADOOP_HOME"] = r"D:\hadoop"
os.environ["PATH"]        = (
    r"C:\Program Files\Eclipse Adoptium\jdk-11.0.31.11-hotspot\bin" + ";" +
    r"D:\hadoop\bin" + ";" +
    os.environ.get("PATH", "")
)
#Kiểm tra hoạt động

# Spark Master UI : http://localhost:8090  → thấy 1 application đang chạy
# Spark Worker UI : http://localhost:8091  → thấy memory/CPU đang dùng
# MinIO UI        : http://localhost:9001  → thấy file xuất hiện trong bronze/clean/
# ─────────────────────────────────────────────
# PHẦN 1 — LOAD ENVIRONMENT VARIABLES
# ─────────────────────────────────────────────
# __file__ = đường dẫn tuyệt đối của file này
# os.path.dirname(__file__) = thư mục spark/
# → tìm file .env trong cùng thư mục spark/
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

# MinIO
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC")

# Paths MinIO
# s3a:// là giao thức Spark dùng để nói chuyện với S3 / MinIO
CLEAN_PATH            = os.getenv("MINIO_CLEAN_PATH")
QUARANTINE_PATH       = os.getenv("MINIO_QUARANTINE_PATH")

# Checkpoint giúp Spark nhớ đã xử lý đến đâu
# nếu job bị crash → khởi động lại đọc tiếp từ chỗ dừng
# không bị mất hay đọc trùng data
CHECKPOINT_CLEAN      = os.getenv("MINIO_CHECKPOINT_CLEAN")
CHECKPOINT_QUARANTINE = os.getenv("MINIO_CHECKPOINT_QUARANTINE")

# Spark config
SPARK_MASTER       = os.getenv("SPARK_MASTER")
SPARK_APP_NAME     = os.getenv("SPARK_APP_NAME",            "SpotifyStreaming")
MAX_OFFSETS        = os.getenv("MAX_OFFSETS_PER_TRIGGER",   "1000")
TRIGGER_CLEAN      = os.getenv("TRIGGER_INTERVAL_CLEAN",    "30 seconds")
TRIGGER_QUARANTINE = os.getenv("TRIGGER_INTERVAL_QUARANTINE","10 seconds")


# ─────────────────────────────────────────────
# PHẦN 2 — TẠO SPARKSESSION
# ─────────────────────────────────────────────
# SparkSession là cổng vào duy nhất để làm việc với Spark
# Mọi thứ đều phải đi qua object này
#
# .config("spark.jars.packages", ...) :
#   Spark tự động tải JAR từ Maven về ~/.ivy2/
#   Lần đầu chạy mất 1-2 phút (cần internet)
#   Lần sau dùng cache → chạy ngay
#
# 3 JAR cần thiết:
#   spark-sql-kafka  : để Spark đọc được Kafka stream
#   hadoop-aws       : để Spark ghi được lên MinIO (S3)
#   aws-java-sdk     : thư viện AWS đi kèm hadoop-aws

#----------------Spark builder ---------------
# path.style.access = true : bắt buộc với MinIO
    # MinIO dùng URL dạng http://minio:9000/bucket/key
    # thay vì dạng AWS chuẩn http://bucket.s3.amazonaws.com/key
# Khai báo class xử lý giao thức s3a://
spark = SparkSession.builder \
    .appName(SPARK_APP_NAME) \
    .master(SPARK_MASTER) \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ) \
    .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY) \
    \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    \
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    ) \
    .getOrCreate()

# Chỉ hiện log WARN trở lên, bỏ qua log INFO rác
spark.sparkContext.setLogLevel("WARN")

print("✅ SparkSession created successfully")
print(f"   Spark version : {spark.version}")
print(f"   Spark master  : {SPARK_MASTER}")
print(f"   Kafka topic   : {KAFKA_TOPIC}")
print(f"   MinIO bucket  : {MINIO_BUCKET}")
print(f"   Clean path    : {CLEAN_PATH}")
print(f"   Quarantine    : {QUARANTINE_PATH}")


# ─────────────────────────────────────────────
# PHẦN 3 — KHAI BÁO SCHEMA
# ─────────────────────────────────────────────
# Schema = bản thiết kế cấu trúc dữ liệu
# Spark dùng schema này để:
#   1. Parse đúng kiểu dữ liệu từng field
#   2. Nếu field sai kiểu → tự động đưa vào _corrupt_record
#   3. Không cần Spark tự đoán schema (tốn thời gian và hay sai)
#
# True ở cuối mỗi field = field này có thể NULL
# (cần True vì dữ liệu thực tế hay thiếu field)

schema = StructType([
    # ── fields cũ ──────────────────────────────
    StructField("event_id",    StringType(),  True),
    # id duy nhất mỗi lần nghe nhạc

    StructField("user_id",     StringType(),  True),
    # id người dùng

    StructField("song_id",     StringType(),  True),
    # id bài hát (uuid5 cố định theo tên bài + nghệ sĩ)

    StructField("artist_name", StringType(),  True),
    StructField("song_name",   StringType(),  True),

    StructField("event_type",  StringType(),  True),
    # play | pause | skip | add_to_playlist

    StructField("device_type", StringType(),  True),
    # mobile | desktop | web

    StructField("country",     StringType(),  True),
    # US | UK | CA | AU | IN | DE | VN | JP

    StructField("timestamp",   StringType(),  True),
    # ISO 8601: 2026-06-06T10:12:33Z

    # ── fields mới: thông tin user ─────────────
    StructField("username",          StringType(),  True),
    StructField("age",               IntegerType(), True),
    StructField("subscription_type", StringType(),  True),
    # free | premium

    StructField("registration_date", StringType(),  True),

    # ── fields mới: thông tin bài hát ──────────
    StructField("genre",             StringType(),  True),
    # Pop | Hip-Hop | K-Pop | Electropop | Country Pop

    StructField("duration_seconds",  IntegerType(), True),
    # độ dài bài hát tính bằng giây

    StructField("release_year",      IntegerType(), True),
    StructField("album_name",        StringType(),  True),
])

print(f"✅ Schema declared: {len(schema.fields)} fields")


# ─────────────────────────────────────────────
# PHẦN 4 — ĐỌC STREAM TỪ KAFKA
# ─────────────────────────────────────────────
# readStream  : đọc dữ liệu liên tục (không phải đọc 1 lần)
# format kafka: dùng JAR spark-sql-kafka đã khai báo ở trên
#
# Kafka trả về mỗi message dưới dạng:
# ┌──────────┬───────────────────────────────────────────┐
# │  key     │  value (bytes)                            │
# │  topic   │  partition                                │
# │  offset  │  timestamp                                │
# └──────────┴───────────────────────────────────────────┘
# Ta chỉ cần cột "value" — đó là JSON event từ Producer

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe",               KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss",  "false") \
    .option("maxOffsetsPerTrigger", MAX_OFFSETS) \
    .load()

print("✅ Kafka readStream configured")


# ─────────────────────────────────────────────
# PHẦN 5 — PARSE JSON + ENRICH DATA
# ─────────────────────────────────────────────
# Bước này biến raw bytes từ Kafka thành DataFrame có cột rõ ràng
#
# Luồng xử lý:
# raw_df (binary)
#   → cast value sang String
#   → from_json parse theo schema
#   → select tất cả fields ra ngoài
#   → thêm cột processed_at (thời điểm Spark xử lý)

parsed_df = raw_df.select(
    from_json(
        col("value").cast("string"),  # binary → string
        schema                         # string → struct theo schema
    ).alias("data"),
    col("timestamp").alias("kafka_ingest_time")
    # thời điểm message vào Kafka (khác với timestamp trong event)
) \
.select(
    "data.*",            # mở tất cả fields trong struct data ra ngoài
    "kafka_ingest_time"  # giữ lại kafka timestamp để debug
) \
.withColumn(
    "processed_at",
    current_timestamp()  # thêm cột ghi nhận thời điểm Spark xử lý
)

print("✅ Parsing and enrichment configured")


# ─────────────────────────────────────────────
# PHẦN 6 — TÁCH CLEAN / DIRTY
# ─────────────────────────────────────────────
# Tương đương với validate_event() trong kafka_to_minio.py cũ
# Nhưng dùng Spark SQL expressions → xử lý song song trên nhiều worker
#
# Điều kiện dữ liệu SẠCH:
#   4 field bắt buộc phải có giá trị (không NULL, không rỗng)

clean_condition = (
    col("event_id").isNotNull()  &
    col("user_id").isNotNull()   &
    col("song_id").isNotNull()   &
    col("timestamp").isNotNull()
)

# DataFrame chứa dữ liệu sạch
clean_df = parsed_df.filter(clean_condition)

# DataFrame chứa dữ liệu bẩn
# ~ nghĩa là NOT (phủ định điều kiện clean)
dirty_df = parsed_df.filter(~clean_condition) \
    .withColumn(
        "_reject_reason",
        # when().when().otherwise() giống IF-ELIF-ELSE
        # Spark kiểm tra từng điều kiện theo thứ tự
        # điều kiện nào đúng trước → lấy giá trị đó
        when(col("event_id").isNull(),   lit("missing_event_id"))
       .when(col("user_id").isNull(),    lit("missing_user_id"))
       .when(col("song_id").isNull(),    lit("missing_song_id"))
       .when(col("timestamp").isNull(),  lit("missing_timestamp"))
       .otherwise(lit("unknown"))
    ) \
    .withColumn(
        "_rejected_at",
        current_timestamp().cast("string")
        # ghi lại thời điểm bị reject để audit sau
    )

print("✅ Clean / Dirty split configured")


# ─────────────────────────────────────────────
# PHẦN 7 — GHI CLEAN STREAM XUỐNG MINIO
# ─────────────────────────────────────────────
# writeStream  : ghi liên tục (không phải ghi 1 lần)
# format json  : mỗi row ghi thành 1 dòng JSON
# outputMode   : append = chỉ ghi dữ liệu mới, không ghi đè
#
# partitionBy("country"):
#   Spark tự tạo thư mục theo country
#   bronze/clean/country=US/part-0001.json
#   bronze/clean/country=VN/part-0002.json
#   → khi dbt query WHERE country='US' chỉ đọc 1 thư mục
#   → nhanh hơn nhiều so với scan toàn bộ file
#
# checkpointLocation:
#   Spark lưu thông tin "đã xử lý đến đâu" vào đây
#   Nếu job crash → restart đọc tiếp từ chỗ dừng
#   KHÔNG bị xử lý trùng hay mất data
#
# trigger(processingTime=TRIGGER_CLEAN):
#   Đọc từ .env → TRIGGER_INTERVAL_CLEAN="30 seconds"
#   Cứ 30 giây Spark gom tất cả message nhận được
#   đóng gói thành 1 micro-batch rồi ghi xuống MinIO

clean_query = clean_df.writeStream \
    .format("json") \
    .option("path",               CLEAN_PATH) \
    .option("checkpointLocation", CHECKPOINT_CLEAN) \
    .outputMode("append") \
    .trigger(processingTime=TRIGGER_CLEAN) \
    .start()

print(f"✅ Clean stream writing to     : {CLEAN_PATH}")
print(f"   Trigger interval            : {TRIGGER_CLEAN}")


# ─────────────────────────────────────────────
# PHẦN 8 — GHI DIRTY STREAM XUỐNG MINIO
# ─────────────────────────────────────────────
# Tương tự clean stream nhưng:
#   - Ghi vào bronze/quarantine/ thay vì bronze/clean/
#   - Không partitionBy vì ít data hơn nhiều
#   - Trigger ngắn hơn (10s) để phát hiện lỗi sớm hơn
#   - TRIGGER_INTERVAL_QUARANTINE đọc từ .env

dirty_query = dirty_df.writeStream \
    .format("json") \
    .option("path",               QUARANTINE_PATH) \
    .option("checkpointLocation", CHECKPOINT_QUARANTINE) \
    .outputMode("append") \
    .trigger(processingTime=TRIGGER_QUARANTINE) \
    .start()

print(f"✅ Quarantine stream writing to: {QUARANTINE_PATH}")
print(f"   Trigger interval            : {TRIGGER_QUARANTINE}")


# ─────────────────────────────────────────────
# PHẦN 9 — GIỮ JOB CHẠY MÃI
# ─────────────────────────────────────────────
# awaitAnyTermination() :
#   Giữ chương trình Python không thoát ra
#   Spark chạy 2 stream song song trên background threads
#   Nếu 1 trong 2 stream bị lỗi/dừng → Python process thoát
#   → Docker container restart (nếu có restart: always)
#
# Nếu KHÔNG có dòng này:
#   Python script chạy xong → thoát ngay
#   Cả 2 stream bị kill theo dù Kafka vẫn còn data

print()
print("🚀 Both streams are running...")
print("   Press Ctrl+C to stop")
print()

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n⛔ Stopping streams...")
    clean_query.stop()
    dirty_query.stop()
    spark.stop()
    print("✅ Spark job stopped cleanly")