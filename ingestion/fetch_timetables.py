import requests
import psycopg
from dotenv import load_dotenv
import os
from datetime import datetime
from .utils import STATION_NAMES, fetch_eva_number, parse_xml

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
    
def save_to_db(conn, eva_no, time_type, parsed_response):
    cur = conn.cursor()
    for train in parsed_response:
        cur.execute(
            f"INSERT INTO raw_timetables \
            (s_id, train_id, station_id, planned_route, {time_type}, type) \
            VALUES \
            (%s, %s, %s, %s, %s, %s)",
            (train["s_id"], train["train_id"], eva_no, train["planned_path"], train[time_type], train["type"])
        )
        conn.commit()
    cur.close()

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
        parsed_planned_response = parse_xml(planned_trips)
        save_to_db(conn, eva_number, "scheduled_time", parsed_planned_response)

    conn.close()

if __name__ == "__main__":
    main()