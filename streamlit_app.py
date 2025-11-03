import os
import streamlit as st
import pandas as pd
import psycopg
import plotly.express as px
from dotenv import load_dotenv

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

conn_string = os.getenv('DATABASE_URL')
if not conn_string:
    st.error("DATABASE_URL environment variable not found!")
    st.stop()

# -----------------------------
# Database connection
# -----------------------------
@st.cache_data(ttl=600)
def load_data():
    """
    Load aggregated hourly delay data from the fct_train_delay_summary table.
    The cache refreshes every 10 minutes (600 seconds).
    """
    conn = psycopg.connect(conn_string)
    query = "SELECT * FROM fct_train_delay_summary"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# -----------------------------
# App Title & Introduction
# -----------------------------
st.title("🚆 Deutsche Bahn Train Delay Dashboard")

st.markdown("""
Welcome to **DeutscheBahnalytics** — a real-time analytics dashboard that visualizes 
train delay data from Deutsche Bahn.  
The data is aggregated from live timetable feeds and processed with **dbt** 
to compute delay metrics.

This dashboard helps explore patterns in train punctuality throughout the day.
""")

# -----------------------------
# Load and display data
# -----------------------------
st.header("📊 Data Overview")

st.markdown("""
This section shows the raw aggregated dataset fetched from the database.  
Each row represents the average delay statistics for a specific hour of the day.  
Columns include:
- **hour_of_day**: The hour (0–23) when trains were scheduled.
- **avg_arrival_delay_min** and **avg_departure_delay_min**: Average delays in minutes.
- **total_delays**: Total number of delayed events recorded for that hour.
""")

df = load_data()
st.dataframe(df)

# -----------------------------
# Average Delays Plot
# -----------------------------
st.header("🕒 Average Delays per Hour")

st.markdown("""
This chart shows how **arrival** and **departure delays** vary by hour of the day.  
You can observe whether delays tend to occur more frequently during morning or evening peaks.
""")

fig_avg_delay = px.bar(
    df,
    x="hour_of_day",
    y=["avg_arrival_delay_min", "avg_departure_delay_min"],
    barmode="group",
    labels={
        "hour_of_day": "Hour of Day",
        "value": "Average Delay (min)",
        "variable": "Delay Type",
        "avg_arrival_delay_min": "Average Arrival Delay (minutes)",
        "avg_departure_delay_min": "Average Departure Delay (minutes)"
    },
    title="Average Arrival & Departure Delays by Hour"
)

fig_avg_delay.update_layout(
    xaxis=dict(
        tickmode="linear",
        dtick=1
    )
)

st.plotly_chart(fig_avg_delay, use_container_width=True)

# -----------------------------
# Delay Counts Plot
# -----------------------------
st.header("📈 Total Delays per Hour")

st.markdown("""
This plot displays the **number of delayed train events** per hour.  
It helps identify periods of the day with the highest frequency of delays — for example,
rush hours or late-night schedules.
""")

fig_count = px.bar(
    df,
    x="hour_of_day",
    y=["total_delays"],
    barmode="group",
    labels={
        "hour_of_day": "Hour of Day",
        "value": "Number of Delays"
    },
    title="Total Delays by Hour"
)

fig_count.update_layout(
    xaxis=dict(
        tickmode="linear",
        dtick=1
    )
)

st.plotly_chart(fig_count, use_container_width=True)

# -----------------------------
# Closing Section
# -----------------------------
st.markdown("""
---
💡 *Data Source:* Deutsche Bahn Timetable API  
⚙️ *Data Pipeline:* Ingested via GitHub Actions → Processed with dbt → Visualized in Streamlit  
📍 *Project Repository:* [DeutscheBahnalytics on GitHub](https://github.com/hemantsirsat/DeutscheBahnalytics)
""")
