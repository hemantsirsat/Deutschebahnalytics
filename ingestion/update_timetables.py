import requests
import psycopg
from dotenv import load_dotenv
import os
from .utils import STATION_NAMES, fetch_eva_number, parse_recent_changes

load_dotenv()

conn_string = os.getenv('DATABASE_URL')

DB_CLIENT_ID = os.getenv('Client_ID')
DB_CLIENT_SECRET = os.getenv('Client_Secret')

RECENT_CHANGE_API = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/rchg/"

headers = {
    "DB-Client-ID": DB_CLIENT_ID,
    "DB-Api-Key": DB_CLIENT_SECRET,
    "accept": "application/xml"
}

def fetch_recent_changes(eva_no):
    response = requests.get(RECENT_CHANGE_API + str(eva_no), headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        print(f"Failed for {eva_no}: {response.status_code}")
        return None

def update_db(conn, eva_number, stops):
    cur = conn.cursor()
    for service_id, stop in stops.items():
        cur.execute(
            "UPDATE raw_timetables \
            SET (actual_arrival_time = %s AND actual_departure_time = %s) \
            Where (service_id = %s AND station_id = %s)",
            (stop["actual_arrival_time"], stop["actual_departure_time"], service_id, eva_number)
        )
        conn.commit()
    cur.close()

def main():
    conn = psycopg.connect(conn_string)

    # Iterate through each station
    # states = fetch_states(conn)
    for station in STATION_NAMES:
        eva_number = fetch_eva_number(conn, station)

        # Fetch recent changes
        recent_changes = fetch_recent_changes(eva_number)
        parsed_recent_changes = parse_recent_changes(recent_changes)
        update_db(conn, eva_number, parsed_recent_changes)

    conn.close()

if __name__ == "__main__":
    main()