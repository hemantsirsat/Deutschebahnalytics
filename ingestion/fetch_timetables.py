import requests
import psycopg
from psycopg.rows import tuple_row
from dotenv import load_dotenv
import os
from datetime import datetime
from .utils import STATION_NAMES, fetch_eva_number, parse_planned_timetable

load_dotenv()

conn_string = os.getenv('DATABASE_URL')

DB_CLIENT_ID = os.getenv('Client_ID')
DB_CLIENT_SECRET = os.getenv('Client_Secret')

# API endpoint
PLANNED_TIMETABLE_API = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/plan/"

headers = {
    "DB-Client-ID": DB_CLIENT_ID,
    "DB-Api-Key": DB_CLIENT_SECRET,
    "accept": "application/xml"
}

def fetch_planned_timetable(eva_no,date,hour):
    response = requests.get(PLANNED_TIMETABLE_API + str(eva_no) + f"/{date}/{hour}", headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        print(f"Failed for {eva_no}: {response.status_code}")
        return None
    
def save_to_db(conn, eva_number, stops):
    data = []
    for service_id, stop in stops.items():
        data.append((
            eva_number,
            service_id,
            stop["train_category"],
            stop["train_number"],
            stop["train_operator"],
            stop["platform"],
            stop["route_before_arrival"],
            stop["route_after_departure"],
            stop["planned_arrival_time"],
            stop["planned_departure_time"],  
        ))

    insert_query = """
        INSERT INTO raw_timetable (
            eva_number,
            service_id,
            train_category,
            train_number,
            train_operator,
            platform,
            route_before_arrival,
            route_after_departure,
            planned_arrival_time,
            planned_departure_time
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (eva_number, service_id, train_category, train_number, train_operator, platform, route_before_arrival, route_after_departure, planned_arrival_time, planned_departure_time) DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.executemany(insert_query, data)
    conn.commit()

def main():
    conn = psycopg.connect(conn_string)
    # Fetch current date and time and convert to string
    dt = datetime.now()
    date_str = dt.strftime('%y%m%d')
    hour_str = dt.strftime('%H')

    # Iterate through each station
    # states = fetch_states(conn)
    for station in STATION_NAMES:
        eva_number = fetch_eva_number(conn, station)

        # Fetch planned timetable
        planned_trips = fetch_planned_timetable(eva_number,date_str,hour_str)
        parsed_planned_response = parse_planned_timetable(planned_trips)
        save_to_db(conn, eva_number, parsed_planned_response)

    conn.close()

if __name__ == "__main__":
    main()