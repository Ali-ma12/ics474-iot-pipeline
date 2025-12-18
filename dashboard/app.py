import pandas as pd
import streamlit as st
import psycopg2
import plotly.express as px


# ---- Page setup ----
st.set_page_config(page_title="ICS474 IoT Pipeline Dashboard", layout="wide")
st.title("ICS474 - Big Data Analytics | Real-Time IoT Pipeline Dashboard")

# ---- DB connection settings (matches your docker-compose) ----
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "iot_db"
DB_USER = "iot_user"
DB_PASS = "iot_pass"

@st.cache_data(ttl=5)
def run_query(query: str):
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# ---- Top metrics ----
col1, col2, col3 = st.columns(3)

device_count = run_query("SELECT COUNT(*) AS count FROM device_data;")["count"][0]
corrupt_count = run_query("SELECT COUNT(*) AS count FROM corrupt_records;")["count"][0]
error_count = run_query("SELECT COUNT(*) AS count FROM consumer_error_log;")["count"][0]

col1.metric("Valid records (device_data)", int(device_count))
col2.metric("Corrupt records", int(corrupt_count))
col3.metric("Consumer errors", int(error_count))

st.divider()

# ---- Latest records table ----
st.subheader("Latest device events")
latest = run_query("""
    SELECT car_id, trip_id, event_timestamp, speed_kmph, fuel_level, engine_temp_c, latitude, longitude
    FROM device_data
    ORDER BY event_timestamp DESC
    LIMIT 20;
""")
st.dataframe(latest, use_container_width=True)

# --- Alerts ---- #
st.subheader("🚨 Recent Alerts (Smart Layer)")
alerts = run_query("""
    SELECT created_at, car_id, trip_id, alert_type, severity, alert_message
    FROM alerts
    ORDER BY created_at DESC
    LIMIT 20;
""")
st.dataframe(alerts, use_container_width=True)

st.divider()

# ---- Speed over time chart ----
st.subheader("Speed over time (last 200 events)")
speed_df = run_query("""
    SELECT event_timestamp, speed_kmph, car_id
    FROM device_data
    ORDER BY event_timestamp DESC
    LIMIT 200;
""")

# ensure datetime
speed_df["event_timestamp"] = pd.to_datetime(speed_df["event_timestamp"])
speed_df = speed_df.sort_values("event_timestamp")

fig = px.line(speed_df, x="event_timestamp", y="speed_kmph", color="car_id")
st.plotly_chart(fig, use_container_width=True)

st.divider()

# ---- Records by car ----
st.subheader("Records by car_id")
by_car = run_query("""
    SELECT car_id, COUNT(*) AS records
    FROM device_data
    GROUP BY car_id
    ORDER BY records DESC;
""")
st.bar_chart(by_car.set_index("car_id"))