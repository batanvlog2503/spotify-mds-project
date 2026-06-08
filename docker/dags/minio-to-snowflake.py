import json
import os
from datetime import datetime, timedelta
from airflow import DAG
# dùng đêr khai báo 1 pipeline tự động
# quy định khi nào pipeline này chạy
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import boto3
# để đọc được cái dữ liệu từ MINIO
import snowflake.connector

#trình kết nối chính thức của python tới snowflake
# khi đọc dữ liệu thô từ minio và clean
# thư viện này sẽ mở ra một đường ống kết nối snowflake
# dế bắn các lệnh sql
from dotenv import load_dotenv


#--------------------------------
#--------------LOAD ENVIRONMENT VARIABLES
#--------------------------------

load_dotenv(dotenv_path="/opt/airflow/dags/.env")
# các file code DAG của bạn sẽ được ném vào trong một môi trường container biệt lập
# ---------------------------------------
# ---------------CONFIGURATION VARIABLES----
# ---------------------------------------------


# -----------MINIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
# địa chỉ của minio
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
# thùng chứa S3 bucket

# ✅ THÊM MỚI: tách 2 prefix riêng thay vì 1 MINIO_PREFIX chung
MINIO_PREFIX_CLEAN      = os.getenv("MINIO_PREFIX_CLEAN",      "bronze/clean/")
MINIO_PREFIX_QUARANTINE = os.getenv("MINIO_PREFIX_QUARANTINE",  "bronze/quarantine/")
# thư mục con bên trong cái giỏ. Dùng để định tuyến
# xem dữ liệu nào sẽ được cất vào đâu để tránh lẫn lộn


#--------------SNOWFLAKE CONFIGURATION---------
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
# tên đặng nhập
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
# password
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
#mã định danh định tuyến của cụm Snowflake( thường có dạng xy12345.ap-southeast-1)
# kiểu địa chỉ của tòa nhà giúp python biết cụm server snowflake của bạn đang nằm ở vùng nào
# trên thế giới AWS, Azure hau GCP
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
# cụm máy tính ảo tính toán
# cung cấp CPU /  Ram dể chạy các câu lệnh SQL
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
#tên của CSDL databse lớn nhất
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
# lớp trung gian Schema dùng để gom nhóm các bảng có cùng chủ đề lại với nhau

# ✅ THÊM MỚI: 2 bảng riêng — clean và quarantine
SNOWFLAKE_TABLE_BRONZE     = os.getenv("SNOWFLAKE_TABLE", "spotify_events_bronze")
#Tên của cái Bảng (Table) cụ thể chứa dữ liệu – nơi có các hàng và các cột.
SNOWFLAKE_TABLE_QUARANTINE = "spotify_events_quarantine"
# bảng chứa dữ liệu bẩn bị reject kèm lý do

# ---------------LOCAL FILE PATH ------------------

LOCAL_TEMP_PATH           = os.getenv("LOCAL_TEMP_PATH", "/tmp/spotify_raw.json")
LOCAL_TEMP_QUARANTINE     = "/tmp/spotify_quarantine.json"
# đóng vai trò như 1 trạm trung chuyển trong quá trình
# chuyển dữ liệu từ Minio sng Snowflake
# LOCAL_TEMP_PATH: Code sẽ ngó vào file .env xem bạn cấu hình đường dẫn cụ thể nà không
# Step 1: Tải file thô từ MinIO về ổ cứng máy Airflow
# [MinIO Bucket] ──────► Tải xuống (Download) ──────► [Ổ cứng máy Airflow: LOCAL_TEMP_PATH]
#
# Step 2: Code Python mở file tạm này ra để đọc, xử lý hoặc kiểm tra dữ liệu
# [Ổ cứng máy Airflow: LOCAL_TEMP_PATH] ──► Mở file (Read & Process) ──► [Bộ nhớ RAM]
#
# Step 3: Đẩy dữ liệu đã xử lý từ file tạm sang Snowflake
# [Ổ cứng máy Airflow: LOCAL_TEMP_PATH] ──────► Đẩy lên (Load/Stage) ──────► [Snowflake Table]


#-------------------------------------------------
#--------------PYTHON TASKS FUNCTIONS-------------
#------------------------------------------------

# ✅ THÊM MỚI: helper dùng chung để đọc file từ MinIO
# tránh lặp code giữa extract_clean và extract_quarantine
def read_from_minio(prefix: str, local_path: str) -> str:
    s3 = boto3.client(
        "s3", # tạo 1 thực thể client sử dụng giao thức S3
        endpoint_url=MINIO_ENDPOINT, # địa chỉ IP
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    response = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)
    # đơn giản cái response là list các file .json đã tổng hợp
    # lấy từ trong MINI_BUCKET , tại thư mục prefix
    #
    # ví dụ trong bucket spotify có rất nhiều file json tổng hợp
    contents = response.get("Contents", [])

    #MINIO trả về danh sách chứa thông tin các file dã tìm thấy, nếu thư mục
    # rỗng thì hàm ,get() trả về mảng rỗng [] để code phía dưới không bị crash
    if not contents:
        print(f"⚠️  No files found in MinIO prefix: {prefix}")
        with open(local_path, "w") as f:
            json.dump([], f)
        return local_path

    all_events = []
    # tạo một mảng trống python để chuẩn bị hứng dữ liệu gom được

    for obj in contents: # trong mỗi object in list contents
        key = obj["Key"] # Key:bronze/date=2026-05-21/hour=13/spotify_events_123.json
        # đường dẫn tuyệt đối
        if not key.endswith(".json"): # chỉ làm việc với json
            continue

        # -> tải về ram và bóc tách dữ liệu
        data = s3.get_object(Bucket=MINIO_BUCKET, Key=key) # return binary
        # thực hiện tải download nội dung  của file cụ thể đó từ MINIO vào bộ
        # nhớ cuẩ code dưới dạng dữ liệu nhị phân
        lines = data["Body"].read().decode("utf-8").splitlines()
        # biến mớ nhị phân thành utf-8
        # lines = [
        #     '{"event_id": "d676d0cf...", "artist_name": "Kanye West", ...}',  # Phần tử 1
        #     '{"event_id": "46a3555d...", "artist_name": "Dua Lipa", ...}',  # Phần tử 2
        #     ...
        #     '{"event_id": "f4f8653b...", "artist_name": "The Weekend", ...}'  # Phần tử 10
        # ]
        for line in lines:
            try:
                all_events.append(json.loads(line))
                # biến dòng chữ thô thành 1 object JSON Dic trong python
                # sau ó ném cái object vào 1 cái rổ chung
            except json.JSONDecodeError: # lỗi Format JSON thì next
                continue

    with open(local_path, "w") as f:
        json.dump(all_events, f)
        # ghi hết tất cả sự kiện all_event

    print(f"✅ Extracted {len(all_events)} events from '{prefix}' → {local_path}")
    return local_path


# vào mino quét sacjk các file dữ liệu thô .json
# mà kafka ném vào đó, gộp chúng lại thaanfh một file duy nhất roiof tai ve airflow
def extract_from_minio(): #khai thác truy xuất
    """
    Extract all .json event files from MINIO -> combine -> save locally
    ✅ THÊM MỚI: đọc từ bronze/clean/ thay vì bronze/ chung
    :return:
    """
    return read_from_minio(MINIO_PREFIX_CLEAN, LOCAL_TEMP_PATH)
# [Kafka] ──► [MinIO (Bronze/clean)] ──► [Hàm extract_from_minio()] ──► [LOCAL_TEMP_PATH]
#                                                                              │
#                                                                              ▼
#                                                                         [Snowflake Bronze]


# ✅ THÊM MỚI: hàm đọc dữ liệu bẩn từ bronze/quarantine/
def extract_quarantine_from_minio():
    """
    Extract dirty/rejected events from MinIO quarantine → save locally
    """
    return read_from_minio(MINIO_PREFIX_QUARANTINE, LOCAL_TEMP_QUARANTINE)
# [MinIO (Bronze/quarantine)] ──► [Hàm extract_quarantine()] ──► [LOCAL_TEMP_QUARANTINE]
#                                                                         │
#                                                                         ▼
#                                                                [Snowflake Quarantine]


# nhiệm vụ
# đọc file tạm chứa mở dữ liệu thu gom từ mINIo lên
# sau đó kết nối snowflake
# tự độn tạo bảng nếu chưa có và chèn dữ liệu thô
# Snowflake là một kho dữ liệu (Data Warehouse) tối ưu cho việc tính toán,
# phân tích báo cáo lớn,
# chứ nó không được thiết kế để hứng 1 triệu lệnh chèn dữ liệu nhỏ lẻ mỗi giây.
def load_raw_to_snowflake(**context):
    """
    load raw data directly into Snowflake Bronze table
    No transformation or clleaning
    ✅ THÊM MỚI: thêm các field mới từ Producer v2
    :param context:
    :return:
    """

    file_path = context["ti"].xcom_pull(task_ids="extract_data")
    # lấy vị trí file tmp
    # /tmp/spotify_raw.json
    with open(file_path, "r") as f:
        events = json.load(f)
    # : Đọc file tạm dạng mảng JSON (10 bài hát lúc nãy) và biến nó thành một
    # Danh sách các Object nằm trong bộ nhớ RAM (biến events).
    if not events:
        print("No Events found to load.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

    cur = conn.cursor()
    # tạo con troeor để khi nào thực hiện query SQL bạn
    # phải dùng contror này thực thi

    cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
    cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

    # ✅ THÊM MỚI: thêm các field mới vào CREATE TABLE
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE_BRONZE} (
            -- fields cũ
            event_id    STRING,
            user_id     STRING,
            song_id     STRING,
            artist_name STRING,
            song_name   STRING,
            event_type  STRING,
            device_type STRING,
            country     STRING,
            timestamp   STRING,
            -- ✅ fields mới: user info
            username          STRING,
            age               INTEGER,
            subscription_type STRING,
            registration_date STRING,
            -- ✅ fields mới: song info
            genre            STRING,
            duration_seconds INTEGER,
            release_year     INTEGER,
            album_name       STRING
        );
    """)

    # ✅ THÊM MỚI: insert thêm 8 field mới
    insert_sql = f"""
        INSERT INTO {SNOWFLAKE_TABLE_BRONZE} (
            event_id, user_id, song_id, artist_name, song_name,
            event_type, device_type, country, timestamp,
            username, age, subscription_type, registration_date,
            genre, duration_seconds, release_year, album_name
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
    """

    for event in events:
        cur.execute(insert_sql, (
            # fields cũ
            event.get("event_id"),
            event.get("user_id"),
            event.get("song_id"),
            event.get("artist_name"),
            event.get("song_name"),
            event.get("event_type"),
            event.get("device_type"),
            event.get("country"),
            event.get("timestamp"),
            # ✅ fields mới: user
            event.get("username"),
            event.get("age"),
            event.get("subscription_type"),
            event.get("registration_date"),
            # ✅ fields mới: song
            event.get("genre"),
            event.get("duration_seconds"),
            event.get("release_year"),
            event.get("album_name"),
        ))

    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ Loaded {len(events)} raw records into Snowflake table: {SNOWFLAKE_TABLE_BRONZE}")
# tác dụng của hàm
# 1. Biến dữ liệu từ "Tạm thời" thành "Vĩnh viễn"
#2. Biến dữ liệu từ dạng "Văn bản" (JSON) thành dạng "Bảng phân tích" (Structured SQL)
#3. Đóng vai trò là Tầng "Bronze" (Dữ liệu thô - Raw Layer) trong kiến trúc Medallion


# ✅ THÊM MỚI: hàm load dữ liệu bẩn vào bảng quarantine riêng
def load_quarantine_to_snowflake(**context):
    """
    Load dirty/rejected events into Snowflake Quarantine table
    Lưu kèm lý do reject để audit sau
    """
    file_path = context["ti"].xcom_pull(task_ids="extract_quarantine_data")
    # lấy vị trí file tmp quarantine
    with open(file_path, "r") as f:
        events = json.load(f)
    # Đọc file tạm dạng mảng JSON và biến thành Danh sách Object trong RAM

    if not events:
        print("✅ No quarantine events — data is clean!")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

    cur = conn.cursor()
    # tạo con trỏ để khi nào thực hiện query SQL bạn
    # phải dùng con trỏ này thực thi

    cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
    cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE_QUARANTINE} (
            event_id    STRING,
            user_id     STRING,
            song_id     STRING,
            artist_name STRING,
            song_name   STRING,
            event_type  STRING,
            device_type STRING,
            country     STRING,
            timestamp   STRING,
            -- lý do bị reject (được gắn bởi kafka_to_minio.py hoặc spark_streaming)
            _reject_reason STRING,
            _rejected_at   STRING
        );
    """)

    insert_sql = f"""
        INSERT INTO {SNOWFLAKE_TABLE_QUARANTINE} (
            event_id, user_id, song_id, artist_name, song_name,
            event_type, device_type, country, timestamp,
            _reject_reason, _rejected_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for event in events:
        cur.execute(insert_sql, (
            event.get("event_id"),
            event.get("user_id"),
            event.get("song_id"),
            event.get("artist_name"),
            event.get("song_name"),
            event.get("event_type"),
            event.get("device_type"),
            event.get("country"),
            event.get("timestamp"),
            event.get("_reject_reason", "unknown"),
            event.get("_rejected_at"),
        ))

    conn.commit()
    cur.close()
    conn.close()

    print(f"⚠️  Loaded {len(events)} quarantine records → Snowflake: {SNOWFLAKE_TABLE_QUARANTINE}")
# tác dụng của hàm
# 1. Lưu dữ liệu bẩn vĩnh viễn thay vì bỏ đi
# 2. Kèm lý do reject để team data engineer audit sau
# 3. Đóng vai trò Quarantine Layer trong kiến trúc Medallion


#------------------------------------------------------------
#-----------------AIRFLOW DAG DEFINITION----------------------
#-------------------------------------------------------------

#Nó quy định: Khi nào hệ thống bắt đầu chạy, nếu lỗi thì xử lý ra sao, và các bước phải chạy theo thứ tự nào.
# kiểu đây là một bản thiết kễ và đợi khi nào hệt thống hoạt động

# cấu hình quản trị và chính sách bảo hiểm (Retry)
default_args = {
    "owner":"airflow",
    # khai báo người quản lý DAG này là ai: airflow
    "start_date":datetime(2025, 10, 21),
    # ngày bắt đầu có hiệu lực với DAG
    # airflow dựa vào mốc này để tính toán ls chạy
    "retries":1,
    # hiểu đơn giản nếu gặp lỗi chập chờn khi chạy Minio và Snowflake
    # airflow k báo thất bại ngay mà  refresh lại 1 lần nữa
    "retry_delay":timedelta(minutes=5)
    # khi lỗi thì đợi 5 phút để đứng lại
    # sau đó mới chạy lại
}

# khung DAG
with DAG(
    "spotify_minio_to_snowflake_bronze",
    # id định danh duy nhất của DAG,
    # hienj trên giao diện UI Airflow
    # nhinf vào tên là biết nhiệm vụ của DAG
    default_args=default_args,
    #
    description="load raw spotify events from MINIO to Snowflake Bronze + Quarantine tables",
    schedule_interval="@hourly",
    catchup=False
) as dag:
    # bên trong DAG dùng PythonOperator - một công cụ
    # airflow chuyên dùng để bọc hàm python thuần túy

    # gom dữ liệu sạch từ MINIO bronze/clean/
    extract_task = PythonOperator(
        task_id="extract_data",
        # tên của task1 trên giao diện đồ họa
        # phài trùng với xcom_pull(task_ids="extract_data")
        python_callable=extract_from_minio
        # chỉ định cho airflow biết khi bấm chạy Task này
        # hãy thực thi extract_from_minio
    )

    # ✅ THÊM MỚI: gom dữ liệu bẩn từ MINIO bronze/quarantine/
    extract_quarantine_task = PythonOperator(
        task_id="extract_quarantine_data",
        python_callable=extract_quarantine_from_minio
    )

    # gửi dữ liệu sạch lên Snowflake Bronze table
    load_task = PythonOperator(
        task_id="load_raw_to_snow_flake",
        # tên của task2
        python_callable=load_raw_to_snowflake,
        # chạy hàm nạp dữ liệu lên Snowflake
        # provide_context=True
        #important!
        # Nó bật tính năng mở kho dữ liệu hệ thống của Airflow,
        #cho phép hàm load_raw_to_snowflake nhận được biến context
        # để từ đó xài được lệnh gọi XCom lấy đường dẫn file tạm.
    )

    # ✅ THÊM MỚI: gửi dữ liệu bẩn lên Snowflake Quarantine table
    load_quarantine_task = PythonOperator(
        task_id="load_quarantine_to_snowflake",
        python_callable=load_quarantine_to_snowflake,
    )
    # ✅ THÊM MỚI: train model Prophet + log vào MLflow
    train_ml_task = BashOperator(
        task_id="train_forecast_model",
        bash_command="python /opt/airflow/scripts/train_forecast.py"
    )
    # -------------------------------------------------------
    # Thứ tự chạy — 2 nhánh song song, độc lập nhau:
    #
    # extract_task (clean)      ──► load_task (clean)
    # extract_quarantine_task   ──► load_quarantine_task
    #
    # nhánh quarantine lỗi KHÔNG ảnh hưởng nhánh clean
    # -------------------------------------------------------
    # ✅ Sửa — gộp lại gọn hơn
    extract_task >> load_task >> train_ml_task
    extract_quarantine_task >> load_quarantine_task