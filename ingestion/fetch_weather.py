import os
import requests
import psycopg
from datetime import datetime
from dotenv import load_dotenv

from .utils import STATION_NAMES

# Load environment variables
load_dotenv()

conn_string = os.getenv('DATABASE_URL')

API_KEY = os.getenv("WEATHER_API_KEY")

def fetch_weather(lat: float, lon: float, lang: str = "en") -> dict:
    """
    Fetch current weather using WeatherAPI.com based on latitude and longitude.
    
    Args:
        lat (float): Latitude of location.
        lon (float): Longitude of location.
        lang (str): Language code for weather description (default: English).
    
    Returns:
        dict: Parsed JSON response containing current weather data.
    """
    if not API_KEY:
        raise ValueError("Missing WEATHER_API_KEY in environment variables.")

    url = "https://api.weatherapi.com/v1/current.json"

    try:
        params = {
            "key": API_KEY,
            "q": f"{lat},{lon}",
            "lang": lang
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Error fetching weather. Error: {e}")
        return None

    current = data["current"]
    return {
        "temperature": current["temp_c"],
        "humidity": current["humidity"],
        "wind": current["wind_kph"],
        "condition": current["condition"]["text"],
        "visibility": current["vis_km"]
    }

def fetch_coordinates(conn, station):
    query = """
        SELECT cordinates from raw_stations WHERE name= %s;
    """
    with conn.cursor() as curr:
        curr.execute(query, (station,))
        result = curr.fetchone()
        if result:
            lon, lat = result[0].replace("(", "").replace(")","").split(",")
            return (float(lat), float(lon)) # saved as lon, lat. We need lat, lon
        return None

def save_to_db(conn, data):
    insert_query = """
        INSERT INTO raw_weather (
            station_name,
            hour,
            temperature,
            humidity,
            wind,
            condition,
            visibility,
            record_time
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    with conn.cursor() as cur:
        cur.executemany(insert_query, data)
    conn.commit()

if __name__ == "__main__":
    dt = datetime.now()
    conn = psycopg.connect(conn_string)
    data = []
    try:
        for station in STATION_NAMES:
            coordinates = fetch_coordinates(conn, station)
            if coordinates:
                latitude, longitude = coordinates
                weather = fetch_weather(latitude, longitude)
                if weather:
                    data.append((
                        station,
                        dt.hour,
                        weather["temperature"],
                        weather["humidity"],
                        weather["wind"],
                        weather["condition"],
                        weather["visibility"],
                        dt
                    ))
    
    except requests.exceptions.RequestException as e:
        print("Error fetching weather data:", e)

    if data:
        save_to_db(conn, data)
