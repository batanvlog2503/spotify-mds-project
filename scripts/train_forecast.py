import mlflow
import mlflow.prophet
import pandas as pd
from prophet import Prophet
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="/opt/airflow/dags/.env")

# Kết nối MLflow — dùng localhost khi chạy local
# dùng http://mlflow:5000 khi chạy trong Docker
mlflow.set_tracking_uri(os.getenv("MLFLOW_URI", "http://localhost:5000"))
mlflow.set_experiment("spotify_song_forecast")

# Kết nối Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema="TRANSFORM"
)

# Đọc từ Gold table
df = pd.read_sql("""
    SELECT
        DATE_TRUNC('day', event_ts) AS ds,
        COUNT(CASE WHEN event_type = 'play' THEN 1 END) AS y
    FROM spotify_silver
    GROUP BY DATE_TRUNC('day', event_ts)
    ORDER BY ds
""", conn)

conn.close()

print(f"✅ Loaded {len(df)} days of data")
print(df.tail(5))

if len(df) < 2:
    print("⚠️  Không đủ data để train model (cần ít nhất 2 ngày)")
    exit(0)

# Train model và log vào MLflow
with mlflow.start_run():
    model = Prophet(
        yearly_seasonality=False,  # data chưa đủ 1 năm
        weekly_seasonality=True,
        daily_seasonality=True
    )
    model.fit(df)

    # Dự báo 7 ngày tới
    future   = model.make_future_dataframe(periods=7)
    forecast = model.predict(future)

    # Tính MAE trên data đã có
    merged = forecast[['ds', 'yhat']].merge(df, on='ds')
    mae    = (merged['yhat'] - merged['y']).abs().mean()
    rmse   = ((merged['yhat'] - merged['y']) ** 2).mean() ** 0.5

    # Log vào MLflow
    mlflow.log_metric("mae",  round(mae,  2))
    mlflow.log_metric("rmse", round(rmse, 2))
    mlflow.log_param("forecast_periods",    7)
    mlflow.log_param("weekly_seasonality",  True)
    mlflow.log_param("daily_seasonality",   True)
    mlflow.prophet.log_model(model, "prophet_model")

    print(f"✅ MAE  : {mae:.2f}")
    print(f"✅ RMSE : {rmse:.2f}")
    print("✅ Model logged to MLflow")