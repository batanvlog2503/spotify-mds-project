import os
import mlflow
import mlflow.prophet
import pandas as pd
from prophet import Prophet
import snowflake.connector
from dotenv import load_dotenv

# Đọc từ dags/.env vì biến Snowflake nằm ở đó
load_dotenv(dotenv_path="/opt/airflow/dags/.env")

# ─────────────────────────────────────────────
# PHẦN 1 — KẾT NỐI MLFLOW
# ─────────────────────────────────────────────
# host.docker.internal = địa chỉ đặc biệt để container
# kết nối ra máy Windows host (nơi MLflow đang chạy)
MLFLOW_URI = os.getenv("MLFLOW_URI", "http://host.docker.internal:5000")
mlflow.set_tracking_uri(MLFLOW_URI)
mlflow.set_experiment("spotify_song_forecast")
print(f"✅ MLflow URI: {MLFLOW_URI}")

# ─────────────────────────────────────────────
# PHẦN 2 — KẾT NỐI SNOWFLAKE
# ─────────────────────────────────────────────
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema="TRANSFORM"
)

# ─────────────────────────────────────────────
# PHẦN 3 — ĐỌC DATA TỪ SNOWFLAKE
# ─────────────────────────────────────────────
# Đọc tổng lượt play theo từng ngày từ Silver table
# Prophet cần 2 cột: ds (date) và y (value)
df = pd.read_sql("""
    SELECT
        DATE_TRUNC('day', event_ts)                      AS ds,
        COUNT(CASE WHEN event_type = 'play' THEN 1 END)  AS y
    FROM spotify_silver
    GROUP BY DATE_TRUNC('day', event_ts)
    ORDER BY ds
""", conn)

conn.close()

# Snowflake trả về tên cột VIẾT HOA → chuẩn hóa về lowercase
df.columns = df.columns.str.lower()

# Prophet không chấp nhận timezone trong cột ds
# Snowflake trả về 2026-06-06 00:00:00+00:00 → cần bỏ +00:00
df['ds'] = pd.to_datetime(df['ds']).dt.tz_localize(None)

print(f"✅ Loaded {len(df)} days of data")
print(df)

# ─────────────────────────────────────────────
# PHẦN 4 — KIỂM TRA ĐỦ DATA
# ─────────────────────────────────────────────
# Prophet cần ít nhất 2 ngày data để train
if len(df) < 2:
    print("⚠️  Không đủ data để train (cần ít nhất 2 ngày)")
    print("   Chờ thêm data từ pipeline rồi chạy lại")
    exit(0)

# ─────────────────────────────────────────────
# PHẦN 5 — TRAIN MODEL + LOG MLFLOW
# ─────────────────────────────────────────────
# mlflow.start_run() mở 1 lần chạy mới trong experiment
# Mọi thứ log bên trong sẽ gắn vào run này
# Khi thoát khỏi with block → run tự động đóng lại

with mlflow.start_run():

    # ── Train Prophet model ──────────────────
    # yearly_seasonality=False : data chưa đủ 1 năm
    # weekly_seasonality=True  : học pattern theo tuần
    # daily_seasonality=True   : học pattern theo giờ trong ngày
    model = Prophet(
        yearly_seasonality=False,
        weekly_seasonality=True,
        daily_seasonality=True
    )
    model.fit(df)
    print("✅ Model trained successfully")

    # ── Dự báo 7 ngày tới ───────────────────
    # make_future_dataframe tạo dataframe gồm
    # toàn bộ ngày đã có + 7 ngày tương lai
    future   = model.make_future_dataframe(periods=7)
    forecast = model.predict(future)

    print("✅ Forecast generated")
    print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(10))

    # ── Tính metrics ─────────────────────────
    # Chỉ tính trên data đã có (không tính 7 ngày tương lai)
    # vì chưa có giá trị thật để so sánh
    merged = forecast[['ds', 'yhat']].merge(df, on='ds')
    mae    = (merged['yhat'] - merged['y']).abs().mean()
    rmse   = ((merged['yhat'] - merged['y']) ** 2).mean() ** 0.5

    # ── Log metrics vào MLflow ───────────────
    # Metrics: số đo chất lượng model
    mlflow.log_metric("mae",              round(mae,  2))
    mlflow.log_metric("rmse",             round(rmse, 2))
    mlflow.log_metric("data_days",        len(df))
    mlflow.log_metric("total_plays",      int(df['y'].sum()))

    # Params: cấu hình của model
    mlflow.log_param("forecast_periods",   7)
    mlflow.log_param("weekly_seasonality", True)
    mlflow.log_param("daily_seasonality",  True)
    mlflow.log_param("yearly_seasonality", False)
    mlflow.log_param("model_type",         "Prophet")

    print()
    print(f"✅ MAE              : {mae:.2f}")
    print(f"✅ RMSE             : {rmse:.2f}")
    print(f"✅ Total days train : {len(df)}")
    print(f"✅ Total plays      : {int(df['y'].sum())}")
    print()
    print("✅ Metrics logged to MLflow successfully!")
    print(f"   View at: {MLFLOW_URI}")