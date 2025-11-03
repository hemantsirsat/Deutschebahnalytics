import xml.etree.ElementTree as ET
from datetime import datetime

STATION_NAMES = [
    "Hamburg Hbf", 
    "Frankfurt (Main) Hbf", 
    "München Hbf", "Köln Hbf", 
    "Berlin Hauptbahnhof", 
    "Düsseldorf Hbf", 
    "Hannover Hbf", 
    "Stuttgart Hbf",
    "Leipzig Hbf",
    "Dresden Hbf",
    "Berlin Friedrichstraße",
    "Braunschweig Hbf"
]

def fetch_eva_number(conn, station):
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT eva_number FROM raw_stations WHERE name='{station}';")
            rows = cur.fetchall()
            return rows[0][0]
    except Exception as e:
        print(f"Error while fetching eva-number for station {station}. Error: {e}")
    return None

def parse_db_time(ts):
    return datetime.strptime(ts, "%y%m%d%H%M") if ts else None

def parse_planned_timetable(response):
    root = ET.fromstring(response)
    stops = {}

    for stop in root.findall("s"):
        service_id = stop.attrib.get("id")
        stop_data = {}

        # Train info
        trip_label = stop.find("tl")
        stop_data["train_category"] = trip_label.attrib.get("c") if trip_label is not None else None
        stop_data["train_number"] = trip_label.attrib.get("n") if trip_label is not None else None
        stop_data["train_operator"] = trip_label.attrib.get("o") if trip_label is not None else None

        # Arrival
        ar = stop.find("ar")
        if ar is not None:
            stop_data["platform"] = ar.attrib.get("pp")
            stop_data["route_before_arrival"] = ar.attrib.get("ppth")
            stop_data["planned_arrival_time"] = parse_db_time(ar.attrib.get("pt"))
        else:
            stop_data["platform"] = None
            stop_data["route_before_arrival"] = None
            stop_data["planned_arrival_time"] = None

        # Departure
        dp = stop.find("dp")
        if dp is not None:
            stop_data["route_after_departure"] = dp.attrib.get("ppth")
            stop_data["planned_departure_time"] = parse_db_time(dp.attrib.get("pt"))
        else:
            stop_data["route_after_departure"] = None
            stop_data["planned_departure_time"] = None

        stops[service_id] = stop_data

    return stops

def parse_recent_changes(response):
    root = ET.fromstring(response)
    stops = {}

    for stop in root.findall("s"):
        service_id = stop.attrib.get("id")
        stop_data = {}

        # Actual arrival
        ar = stop.find("ar")
        if ar is not None:
            stop_data["actual_arrival_time"] = parse_db_time(ar.attrib.get("ct"))
        else:
            stop_data["actual_arrival_time"] = None

        # Actual departure
        dp = stop.find("dp")
        if dp is not None:
            stop_data["actual_departure_time"] = parse_db_time(dp.attrib.get("ct"))
        else:
            stop_data["actual_departure_time"] = None

        stops[service_id] = stop_data

    return stops

# def fetch_states(conn):
#     try:
#         with conn.cursor() as cur:
#             cur.execute("SELECT DISTINCT federal_state FROM raw_stations;")
#             rows = cur.fetchall()
#             return rows
#     except Exception as e:
#         print(f"Error while fetching federal states. Error: {e}")
#     return None
