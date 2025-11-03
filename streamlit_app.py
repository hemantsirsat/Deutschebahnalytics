import os
import streamlit as st
import pandas as pd
import psycopg
import plotly.express as px
from dotenv import load_dotenv

load_dotenv()

conn_string = os.getenv('DATABASE_URL')

# -----------------------------
# Database connection
# -----------------------------
@st.cache_data(ttl=600)
def load_data():
    conn = psycopg.connect(
        conn_string
    )
    query = "SELECT * FROM fct_train_delay_summary"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# -----------------------------
# Load data
# -----------------------------
st.title("Deutsche Bahn Train Delay Dashboard")
df = load_data()

# Show raw data
st.subheader("Raw Hourly Delay Data")
st.dataframe(df)

# -----------------------------
# Average Delays Plot
# -----------------------------
st.subheader("Average Delays per Hour (minutes)")

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
st.subheader("Number of Delays per Hour")

fig_count = px.bar(
    df,
    x="hour_of_day",
    y=["total_delays"],
    barmode="group",
    labels={"hour_of_day": "Hour of Day", "value": "Count", "variable": "Delay Type"},
    title="Total Delays by Hour"
)

# Force all x-axis labels to show
fig_count.update_layout(
    xaxis=dict(
        tickmode="linear",
        dtick=1
    )
)

st.plotly_chart(fig_count, use_container_width=True)