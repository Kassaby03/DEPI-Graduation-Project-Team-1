import streamlit as st
from kafka import KafkaConsumer
import pandas as pd
import json
import base64
import time

# ---------------------------
# Helper Functions
# ---------------------------
def decode_base64_safe(b, default=None):
    """Decode Base64 to float; return default if empty/invalid"""
    if not b or b in ["AA==", ""]:
        return default
    try:
        return float(base64.b64decode(b).decode())
    except:
        return default

def consume_topic(topic_name, bootstrap_servers, fields_base64=[], include_snapshot=False):
    """
    Consume messages from Kafka topic and return DataFrame
    include_snapshot=True => include op='r' for Dim tables
    """
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=5000
    )
    records = []
    for msg in consumer:
        rec = msg.value
        op_type = rec.get("op")
        if include_snapshot:
            if op_type not in ["c","u","r"]:
                continue
        else:
            if op_type not in ["c","u"]:
                continue
        after = rec.get("after", {})
        if after:
            row = {}
            for k, v in after.items():
                if k in fields_base64:
                    row[k] = decode_base64_safe(v)
                else:
                    row[k] = v
            records.append(row)
    return pd.DataFrame(records)

# ---------------------------
# Streamlit Config
# ---------------------------
st.set_page_config(page_title="Weather Kafka CDC Dashboard", layout="wide")
st.title("🌦️ Weather Kafka CDC Real-Time Dashboard")

bootstrap_servers = ["localhost:9092"]

# ---------------------------
# Consume Dim tables first (include snapshot)
# ---------------------------
decimal_cols = ["temperature","humidity","wind_speed","wind_direction",
                "pressure","precipitation","visibility","cloud_coverage"]

dim_location = consume_topic("KASSABY.dbo.DimLocation", bootstrap_servers, include_snapshot=True)
dim_time     = consume_topic("KASSABY.dbo.DimTime", bootstrap_servers, include_snapshot=True)
dim_event    = consume_topic("KASSABY.dbo.DimWeatherEvent", bootstrap_servers, include_snapshot=True)
dim_model    = consume_topic("KASSABY.dbo.DimForecastModel", bootstrap_servers, include_snapshot=True)

# ---------------------------
# Placeholder
# ---------------------------
placeholder = st.empty()
all_fact = []

consumer_fact = KafkaConsumer(
    "KASSABY.dbo.FactWeatherObservation",
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

# ---------------------------
# Real-Time Loop
# ---------------------------
for msg in consumer_fact:
    rec = msg.value
    if rec.get("op") in ["c","u"]:
        after = rec.get("after", {})
        if after:
            row = {}
            for k, v in after.items():
                if k in decimal_cols:
                    row[k] = decode_base64_safe(v)
                else:
                    row[k] = v
            all_fact.append(row)

            df_fact = pd.DataFrame(all_fact)

            # Merge only if Dim tables موجودة و فيها الأعمدة المطلوبة
            df = df_fact.copy()
            if not dim_location.empty and "location_id" in dim_location.columns:
                df = df.merge(dim_location, on="location_id", how="left")
            if not dim_time.empty and "time_id" in dim_time.columns:
                df = df.merge(dim_time, on="time_id", how="left")
            if not dim_event.empty and "event_id" in dim_event.columns:
                df = df.merge(dim_event, on="event_id", how="left")
            if not dim_model.empty and "model_id" in dim_model.columns:
                df = df.merge(dim_model, on="model_id", how="left")

            # Convert decimal columns safely
            for col in decimal_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Fill NaNs for Dim info
            df.fillna({
                "Weather_condition":"Unknown",
                "country":"Unknown",
                "city":"Unknown",
                "season":"Unknown",
                "model_name":"Unknown"
            }, inplace=True)

            # ---------------------------
            # Dashboard Display
            # ---------------------------
            with placeholder.container():
                st.subheader("📊 CDC Records (Latest at Bottom)")
                st.dataframe(df, use_container_width=True)

                # Top KPIs
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Total Records", len(df))
                col2.metric("Avg Temperature", f"{df['temperature'].mean():.2f}" if not df.empty else 0)
                col3.metric("Avg Humidity", f"{df['humidity'].mean():.2f}" if not df.empty else 0)
                col4.metric("Avg Wind Speed", f"{df['wind_speed'].mean():.2f}" if not df.empty else 0)

                # Charts
                st.subheader("🌡 Temperature Trend")
                st.line_chart(df["temperature"])

                st.subheader("☁ Weather Events Count")
                st.bar_chart(df["Weather_condition"].value_counts())

            time.sleep(0.5)
