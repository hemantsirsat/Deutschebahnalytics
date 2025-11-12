import os
import streamlit as st
import pandas as pd
import psycopg
import plotly.express as px
from dotenv import load_dotenv
from ingestion.utils import STATION_NAMES

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
def load_data(table_name):
    """
    Load aggregated hourly delay data from the fct_train_delay_summary table.
    The cache refreshes every 10 minutes (600 seconds).
    """
    conn = psycopg.connect(conn_string)
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

@st.cache_data(ttl=600)
def load_station_day_hour_data(selected_date, selected_station):
    """
    Load daily hourly delay data for a specific station and date from fct_station_day_hour_summary.
    The cache refreshes every 10 minutes (600 seconds).
    """
    conn = psycopg.connect(conn_string)
    query = """
        SELECT 
            date, 
            hour, 
            station_name, 
            avg_arrival_delay_min, 
            avg_departure_delay_min
        FROM fct_station_day_hour_summary
        WHERE date = %s AND station_name = %s
        ORDER BY hour
    """
    df = pd.read_sql(query, conn, params=(selected_date, selected_station))
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

df_train_delay = load_data("fct_train_delay_summary")
st.dataframe(df_train_delay)

# -----------------------------
# Average Delays Plot
# -----------------------------
st.header("🕒 Average Delays per Hour")

st.markdown("""
This chart shows how **arrival** and **departure delays** vary by hour of the day.  
You can observe whether delays tend to occur more frequently during morning or evening peaks.
""")

fig_avg_delay = px.bar(
    df_train_delay,
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

# Dynamic insights for average delays per hour
st.subheader("📊 Insights: Average Delays by Hour")
if not df_train_delay.empty:
    # Find peak hours
    max_arrival_hour = df_train_delay.loc[df_train_delay['avg_arrival_delay_min'].idxmax()]
    min_arrival_hour = df_train_delay.loc[df_train_delay['avg_arrival_delay_min'].idxmin()]
    max_departure_hour = df_train_delay.loc[df_train_delay['avg_departure_delay_min'].idxmax()]
    min_departure_hour = df_train_delay.loc[df_train_delay['avg_departure_delay_min'].idxmin()]
    
    overall_avg_arrival = df_train_delay['avg_arrival_delay_min'].mean()
    overall_avg_departure = df_train_delay['avg_departure_delay_min'].mean()
    
    # Find peak hours (handle ties)
    max_arr_val = df_train_delay['avg_arrival_delay_min'].max()
    min_arr_val = df_train_delay['avg_arrival_delay_min'].min()
    max_dep_val = df_train_delay['avg_departure_delay_min'].max()
    min_dep_val = df_train_delay['avg_departure_delay_min'].min()
    
    peak_arrival_hours = df_train_delay[df_train_delay['avg_arrival_delay_min'] == max_arr_val]['hour_of_day'].tolist()
    best_arrival_hours = df_train_delay[df_train_delay['avg_arrival_delay_min'] == min_arr_val]['hour_of_day'].tolist()
    peak_departure_hours = df_train_delay[df_train_delay['avg_departure_delay_min'] == max_dep_val]['hour_of_day'].tolist()
    best_departure_hours = df_train_delay[df_train_delay['avg_departure_delay_min'] == min_dep_val]['hour_of_day'].tolist()
    
    peak_arr_str = ", ".join([f"{h}:00" for h in peak_arrival_hours])
    best_arr_str = ", ".join([f"{h}:00" for h in best_arrival_hours])
    peak_dep_str = ", ".join([f"{h}:00" for h in peak_departure_hours])
    best_dep_str = ", ".join([f"{h}:00" for h in best_departure_hours])
    
    insights_hourly = f"""
    - **Peak Arrival Delays:** {peak_arr_str} with {max_arr_val:.1f} min average delay
    - **Best Arrival Performance:** {best_arr_str} with {min_arr_val:.1f} min average delay
    - **Peak Departure Delays:** {peak_dep_str} with {max_dep_val:.1f} min average delay
    - **Best Departure Performance:** {best_dep_str} with {min_dep_val:.1f} min average delay
    - **Overall Average Arrival Delay:** {overall_avg_arrival:.1f} min
    - **Overall Average Departure Delay:** {overall_avg_departure:.1f} min
    """
    st.markdown(insights_hourly)

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
    df_train_delay,
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

# Dynamic insights for total delays per hour
st.subheader("📈 Insights: Delay Frequency by Hour")
if not df_train_delay.empty:
    # Find peak and off-peak hours
    max_delays_count = df_train_delay['total_delays'].max()
    min_delays_count = df_train_delay['total_delays'].min()
    
    peak_delay_hours = df_train_delay[df_train_delay['total_delays'] == max_delays_count]['hour_of_day'].tolist()
    off_peak_hours = df_train_delay[df_train_delay['total_delays'] == min_delays_count]['hour_of_day'].tolist()
    
    total_all_delays = df_train_delay['total_delays'].sum()
    avg_delays_per_hour = df_train_delay['total_delays'].mean()
    
    peak_pct = (max_delays_count / total_all_delays * 100) if total_all_delays > 0 else 0
    off_peak_pct = (min_delays_count / total_all_delays * 100) if total_all_delays > 0 else 0
    
    peak_str = ", ".join([f"{h}:00" for h in peak_delay_hours])
    off_peak_str = ", ".join([f"{h}:00" for h in off_peak_hours])
    
    insights_freq = f"""
    - **Peak Delay Hours:** {peak_str} with {int(max_delays_count)} total delays ({peak_pct:.1f}% of all delays)
    - **Off-Peak Hours:** {off_peak_str} with {int(min_delays_count)} total delays ({off_peak_pct:.1f}% of all delays)
    - **Total Delays Across All Hours:** {int(total_all_delays)} events
    - **Average Delays per Hour:** {avg_delays_per_hour:.0f} events
    """
    st.markdown(insights_freq)

# -----------------------------
# Station-level Delay Overview
# -----------------------------
st.header("🚉 Station Delay Comparison")

st.markdown("""
This visualization compares **average arrival and departure delays** with the **total number of delays** 
for each major station.  
It helps identify which stations experience the **longest delays** or the **most frequent disruptions**.
""")

df_station_data = load_data("fct_station_delay_summary")

# Melt the DataFrame to long format for grouped bars
station_melted = df_station_data.melt(
    id_vars=["station_name", "total_delays"],
    value_vars=["avg_arrival_delay_min", "avg_departure_delay_min"],
    var_name="Delay Type",
    value_name="Average Delay (min)"
)

# Create grouped bar chart with color and size encoding
fig_station = px.scatter(
    station_melted,
    x="station_name",
    y="Average Delay (min)",
    color="Delay Type",
    size="total_delays",
    hover_data={"total_delays": True, "station_name": True},
    title="Average Arrival & Departure Delays per Station (Bubble size = Total Delays)"
)

fig_station.update_layout(
    xaxis=dict(title="Station", tickangle=45),
    yaxis_title="Average Delay (minutes)",
    legend_title="Delay Type",
    height=600
)

st.plotly_chart(fig_station, use_container_width=True)

# Dynamic insights for station delays
st.subheader("🚉 Insights: Station Performance")
if not df_station_data.empty:
    # Find worst and best performing stations
    max_arrival_delay = df_station_data['avg_arrival_delay_min'].max()
    min_arrival_delay = df_station_data['avg_arrival_delay_min'].min()
    max_departure_delay = df_station_data['avg_departure_delay_min'].max()
    min_departure_delay = df_station_data['avg_departure_delay_min'].min()
    max_total_delays = df_station_data['total_delays'].max()
    min_total_delays = df_station_data['total_delays'].min()
    
    worst_arrival_stations = df_station_data[df_station_data['avg_arrival_delay_min'] == max_arrival_delay]['station_name'].tolist()
    best_arrival_stations = df_station_data[df_station_data['avg_arrival_delay_min'] == min_arrival_delay]['station_name'].tolist()
    worst_departure_stations = df_station_data[df_station_data['avg_departure_delay_min'] == max_departure_delay]['station_name'].tolist()
    best_departure_stations = df_station_data[df_station_data['avg_departure_delay_min'] == min_departure_delay]['station_name'].tolist()
    most_disrupted_stations = df_station_data[df_station_data['total_delays'] == max_total_delays]['station_name'].tolist()
    least_disrupted_stations = df_station_data[df_station_data['total_delays'] == min_total_delays]['station_name'].tolist()
    
    overall_avg_arrival = df_station_data['avg_arrival_delay_min'].mean()
    overall_avg_departure = df_station_data['avg_departure_delay_min'].mean()
    total_all_station_delays = df_station_data['total_delays'].sum()
    
    worst_arr_str = ", ".join(worst_arrival_stations)
    best_arr_str = ", ".join(best_arrival_stations)
    worst_dep_str = ", ".join(worst_departure_stations)
    best_dep_str = ", ".join(best_departure_stations)
    most_disrupted_str = ", ".join(most_disrupted_stations)
    least_disrupted_str = ", ".join(least_disrupted_stations)
    
    insights_station = f"""
    - **Worst Arrival Performance:** {worst_arr_str} with {max_arrival_delay:.1f} min average delay
    - **Best Arrival Performance:** {best_arr_str} with {min_arrival_delay:.1f} min average delay
    - **Worst Departure Performance:** {worst_dep_str} with {max_departure_delay:.1f} min average delay
    - **Best Departure Performance:** {best_dep_str} with {min_departure_delay:.1f} min average delay
    - **Most Disrupted Station:** {most_disrupted_str} with {int(max_total_delays)} total delays
    - **Least Disrupted Station:** {least_disrupted_str} with {int(min_total_delays)} total delays
    - **Overall Average Arrival Delay:** {overall_avg_arrival:.1f} min
    - **Overall Average Departure Delay:** {overall_avg_departure:.1f} min
    - **Total Delays Across All Stations:** {int(total_all_station_delays)} events
    """
    st.markdown(insights_station)

st.markdown("""
💡 **How to read this chart:**
- **Bubble size** → shows how many delays occurred at the station.  
- **Y-axis** → average delay duration (in minutes).  
- Compare both **arrival** and **departure** delays side-by-side to spot patterns — e.g.,  
  if a station tends to have long arrivals but punctual departures.
""")

# # -----------------------------
# # Train Operator Performance
# # -----------------------------
# st.header("🚂 Train Operator Performance")

# st.markdown("""
# This visualization compares **average delays** and **delay frequency** across different train operators.  
# It helps identify which operators maintain better punctuality and which ones experience more disruptions.
# """)

# df_operator_data = load_data("fct_operator_delay_summary")

# # Create grouped bar chart for operators
# fig_operator = px.bar(
#     df_operator_data,
#     x="train_operator",
#     y=["avg_arrival_delay_min", "avg_departure_delay_min"],
#     barmode="group",
#     labels={
#         "train_operator": "Train Operator",
#         "value": "Average Delay (min)",
#         "variable": "Delay Type",
#         "avg_arrival_delay_min": "Average Arrival Delay (minutes)",
#         "avg_departure_delay_min": "Average Departure Delay (minutes)"
#     },
#     title="Average Arrival & Departure Delays by Train Operator",
#     color_discrete_map={
#         "avg_arrival_delay_min": "#636EFA",
#         "avg_departure_delay_min": "#EF553B"
#     }
# )

# fig_operator.update_layout(
#     xaxis=dict(title="Train Operator", tickangle=45),
#     yaxis_title="Average Delay (minutes)",
#     legend_title="Delay Type",
#     height=500
# )

# st.plotly_chart(fig_operator, use_container_width=True)

# # Operator delay frequency
# fig_operator_count = px.bar(
#     df_operator_data,
#     x="train_operator",
#     y="total_delays",
#     labels={
#         "train_operator": "Train Operator",
#         "total_delays": "Number of Delays"
#     },
#     title="Total Delays by Train Operator",
#     color="total_delays",
#     color_continuous_scale="Reds"
# )

# fig_operator_count.update_layout(
#     xaxis=dict(title="Train Operator", tickangle=45),
#     yaxis_title="Total Delays",
#     height=500
# )

# st.plotly_chart(fig_operator_count, use_container_width=True)

# -----------------------------
# Train Category Analysis
# -----------------------------
st.header("🚄 Train Category Analysis")

st.markdown("""
This section breaks down delays by **train category** (e.g., ICE, Regional, S-Bahn, etc.).  
It reveals whether certain train types are more prone to delays than others.
""")

df_category_data = load_data("fct_train_category_delay_summary")

# Create grouped bar chart for categories
fig_category = px.bar(
    df_category_data,
    x="train_category",
    y=["avg_arrival_delay_min", "avg_departure_delay_min"],
    barmode="group",
    labels={
        "train_category": "Train Category",
        "value": "Average Delay (min)",
        "variable": "Delay Type",
        "avg_arrival_delay_min": "Average Arrival Delay (minutes)",
        "avg_departure_delay_min": "Average Departure Delay (minutes)"
    },
    title="Average Arrival & Departure Delays by Train Category",
    color_discrete_map={
        "avg_arrival_delay_min": "#636EFA",
        "avg_departure_delay_min": "#EF553B"
    }
)

fig_category.update_layout(
    xaxis=dict(title="Train Category", tickangle=45),
    yaxis_title="Average Delay (minutes)",
    legend_title="Delay Type",
    height=500
)

st.plotly_chart(fig_category, use_container_width=True)

# Dynamic conclusion for average delays
st.subheader("📊 Insights: Average Delays by Category")
if not df_category_data.empty:
    # Find worst and best performing categories (handling ties)
    max_arrival_delay = df_category_data['avg_arrival_delay_min'].max()
    min_arrival_delay = df_category_data['avg_arrival_delay_min'].min()
    max_departure_delay = df_category_data['avg_departure_delay_min'].max()
    min_departure_delay = df_category_data['avg_departure_delay_min'].min()
    
    worst_arrival_cats = df_category_data[df_category_data['avg_arrival_delay_min'] == max_arrival_delay]['train_category'].tolist()
    best_arrival_cats = df_category_data[df_category_data['avg_arrival_delay_min'] == min_arrival_delay]['train_category'].tolist()
    worst_departure_cats = df_category_data[df_category_data['avg_departure_delay_min'] == max_departure_delay]['train_category'].tolist()
    best_departure_cats = df_category_data[df_category_data['avg_departure_delay_min'] == min_departure_delay]['train_category'].tolist()
    
    avg_arrival = df_category_data['avg_arrival_delay_min'].mean()
    avg_departure = df_category_data['avg_departure_delay_min'].mean()
    
    # Format category lists
    worst_arrival_str = ", ".join(worst_arrival_cats)
    best_arrival_str = ", ".join(best_arrival_cats)
    worst_departure_str = ", ".join(worst_departure_cats)
    best_departure_str = ", ".join(best_departure_cats)
    
    conclusion = f"""
    - **Worst Arrival Performance:** {worst_arrival_str} with {max_arrival_delay:.1f} min average delay
    - **Best Arrival Performance:** {best_arrival_str} with {min_arrival_delay:.1f} min average delay
    - **Worst Departure Performance:** {worst_departure_str} with {max_departure_delay:.1f} min average delay
    - **Best Departure Performance:** {best_departure_str} with {min_departure_delay:.1f} min average delay
    - **Overall Average Arrival Delay:** {avg_arrival:.1f} min
    - **Overall Average Departure Delay:** {avg_departure:.1f} min
    """
    st.markdown(conclusion)

# Category delay frequency
fig_category_count = px.bar(
    df_category_data,
    x="train_category",
    y="total_delays",
    labels={
        "train_category": "Train Category",
        "total_delays": "Number of Delays"
    },
    title="Total Delays by Train Category",
    color="total_delays",
    color_continuous_scale="Blues"
)

fig_category_count.update_layout(
    xaxis=dict(title="Train Category", tickangle=45),
    yaxis_title="Total Delays",
    height=500
)

st.plotly_chart(fig_category_count, use_container_width=True)

# Dynamic conclusion for delay frequency
st.subheader("📈 Insights: Delay Frequency by Category")
if not df_category_data.empty:
    # Handle ties for most and least delays
    max_delays_count = df_category_data['total_delays'].max()
    min_delays_count = df_category_data['total_delays'].min()
    
    most_delays_cats = df_category_data[df_category_data['total_delays'] == max_delays_count]['train_category'].tolist()
    least_delays_cats = df_category_data[df_category_data['total_delays'] == min_delays_count]['train_category'].tolist()
    
    total_all_delays = df_category_data['total_delays'].sum()
    
    most_delays_pct = (max_delays_count / total_all_delays * 100) if total_all_delays > 0 else 0
    least_delays_pct = (min_delays_count / total_all_delays * 100) if total_all_delays > 0 else 0
    
    # Format category lists
    most_delays_str = ", ".join(most_delays_cats)
    least_delays_str = ", ".join(least_delays_cats)
    
    conclusion_freq = f"""
    - **Most Frequent Delays:** {most_delays_str} with {int(max_delays_count)} total delays ({most_delays_pct:.1f}% of all delays)
    - **Least Frequent Delays:** {least_delays_str} with {int(min_delays_count)} total delays ({least_delays_pct:.1f}% of all delays)
    - **Total Delays Across All Categories:** {int(total_all_delays)} events
    - **Average Delays per Category:** {int(total_all_delays / len(df_category_data)):.0f} events
    """
    st.markdown(conclusion_freq)

# -----------------------------
# Daily Station Delay Analysis
# -----------------------------
st.header("📅 Daily Station Delay Analysis")

st.markdown("""
This section allows you to explore **hourly delay patterns** for a specific station on a selected date.  
Choose a date and station to view how delays vary throughout the day.
""")

# Create two columns for date and station selection
col1, col2 = st.columns(2)

with col1:
    selected_date = st.date_input(
        "Select Date",
        value=pd.Timestamp.now().date(),
        help="Choose a date to analyze delays"
    )

with col2:
    selected_station = st.selectbox(
        "Select Station",
        options=STATION_NAMES,
        help="Choose a station from the list"
    )

# Fetch and display data
try:
    df_station_day_hour = load_station_day_hour_data(selected_date, selected_station)
    
    if df_station_day_hour.empty:
        st.warning(f"No data available for {selected_station} on {selected_date}. Please select a different date or station.")
    else:
        # Display data table
        st.subheader(f"Hourly Delays for {selected_station} on {selected_date}")
        st.dataframe(df_station_day_hour, use_container_width=True)
        
        # Create bar chart for average delays by hour
        fig_daily = px.bar(
            df_station_day_hour,
            x="hour",
            y=["avg_arrival_delay_min", "avg_departure_delay_min"],
            barmode="group",
            labels={
                "hour": "Hour of Day",
                "value": "Average Delay (min)",
                "variable": "Delay Type",
                "avg_arrival_delay_min": "Average Arrival Delay (minutes)",
                "avg_departure_delay_min": "Average Departure Delay (minutes)"
            },
            title=f"Hourly Average Delays - {selected_station} ({selected_date})",
            color_discrete_map={
                "avg_arrival_delay_min": "#636EFA",
                "avg_departure_delay_min": "#EF553B"
            }
        )
        
        fig_daily.update_layout(
            xaxis=dict(
                tickmode="linear",
                dtick=1,
                title="Hour of Day (0-23)"
            ),
            yaxis_title="Average Delay (minutes)",
            legend_title="Delay Type",
            height=500,
            hovermode="x unified"
        )
        
        st.plotly_chart(fig_daily, use_container_width=True)
        
        # Display insights
        st.subheader("📊 Insights")
        if not df_station_day_hour.empty:
            max_arrival_delay = df_station_day_hour['avg_arrival_delay_min'].max()
            min_arrival_delay = df_station_day_hour['avg_arrival_delay_min'].min()
            max_departure_delay = df_station_day_hour['avg_departure_delay_min'].max()
            min_departure_delay = df_station_day_hour['avg_departure_delay_min'].min()
            
            max_arrival_hour = df_station_day_hour[df_station_day_hour['avg_arrival_delay_min'] == max_arrival_delay]['hour'].values[0]
            min_arrival_hour = df_station_day_hour[df_station_day_hour['avg_arrival_delay_min'] == min_arrival_delay]['hour'].values[0]
            max_departure_hour = df_station_day_hour[df_station_day_hour['avg_departure_delay_min'] == max_departure_delay]['hour'].values[0]
            min_departure_hour = df_station_day_hour[df_station_day_hour['avg_departure_delay_min'] == min_departure_delay]['hour'].values[0]
            
            avg_arrival = df_station_day_hour['avg_arrival_delay_min'].mean()
            avg_departure = df_station_day_hour['avg_departure_delay_min'].mean()
            
            insights_daily = f"""
            - **Peak Arrival Delay:** {max_arrival_delay:.1f} min at {int(max_arrival_hour)}:00
            - **Best Arrival Performance:** {min_arrival_delay:.1f} min at {int(min_arrival_hour)}:00
            - **Peak Departure Delay:** {max_departure_delay:.1f} min at {int(max_departure_hour)}:00
            - **Best Departure Performance:** {min_departure_delay:.1f} min at {int(min_departure_hour)}:00
            - **Average Arrival Delay:** {avg_arrival:.1f} min
            - **Average Departure Delay:** {avg_departure:.1f} min
            """
            st.markdown(insights_daily)
            
except Exception as e:
    st.error(f"Error loading data: {str(e)}")

# -----------------------------
# Closing Section
# -----------------------------
st.markdown("""
---
💡 *Data Source:* Deutsche Bahn Timetable API  
⚙️ *Data Pipeline:* Ingested via GitHub Actions → Processed with dbt → Visualized in Streamlit  
📍 *Project Repository:* [DeutscheBahnalytics on GitHub](https://github.com/hemantsirsat/DeutscheBahnalytics)
""")
st.markdown("The data collection began on 12.11.2025 around 19:00 CET (+01:00 UTC). The numbers are updated every 30 minutes.")
