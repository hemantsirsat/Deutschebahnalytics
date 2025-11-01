import requests
import psycopg
from dotenv import load_dotenv
import os

load_dotenv()

conn_string = os.getenv('DATABASE_URL')

DB_CLIENT_ID = os.getenv('Client_ID')
DB_CLIENT_SECRET = os.getenv('Client_Secret')

# API endpoint
STADA_API_URL = "https://apis.deutschebahn.com/db-api-marketplace/apis/station-data/v2/stations"
headers = {
    "DB-Client-ID": DB_CLIENT_ID,
    "DB-Api-Key": DB_CLIENT_SECRET,
    "accept": "application/json"
}

try:
    # Fetch stations
    response = requests.get(STADA_API_URL, headers=headers)
    stations = response.json()

    stations = stations["result"]

    try:
        with psycopg.connect(conn_string) as conn:
            print("Connection established")
            
            with conn.cursor() as cur:
                for station in stations:
                    cur.execute(
                        "INSERT INTO raw_stations (id, name, city, cordinates, zipcode, federal_state, eva_number) \
                        VALUES (%s,%s,%s,%s,%s,%s,%s)", 
                        (
                            station['number'], 
                            station['name'], 
                            station['mailingAddress']['city'], 
                            f"({station["evaNumbers"][0]["geographicCoordinates"]["coordinates"][0]}, {station["evaNumbers"][0]["geographicCoordinates"]["coordinates"][1]})",
                            station['mailingAddress']['zipcode'], 
                            station['federalState'], 
                            station['evaNumbers'][0]["number"]
                        )
                    )
                    conn.commit()
                    print(f"Data for {station['name']} inserted successfully")
        
    except Exception as e:
        print("Error inserting stations: ", e)
except Exception as e:
    print("Error fetching stations: ", e)
# conn.commit()
cur.close()
conn.close()
print("Stations updated successfully")
