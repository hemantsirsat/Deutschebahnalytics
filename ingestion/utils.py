import xml.etree.ElementTree as ET
from datetime import datetime

STATION_NAMES = ["Hamburg Hbf", "München Hbf", "Köln Hbf", "Berlin Hauptbahnhof", "Braunschweig Hbf"]

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
        stops[service_id] = {}

        # Get train info
        trip_label = stop.find("tl")
        stops[service_id]["train_category"] = trip_label.attrib.get("c") if trip_label is not None else None
        stops[service_id]["train_number"] = trip_label.attrib.get("n") if trip_label is not None else None
        stops[service_id]["train_operator"] = trip_label.attrib.get("o") if trip_label is not None else None
        
        for ar in stop.findall("ar"):
            try:
                try:
                    stops[service_id]["platform"] = ar.attrib.get("pp")
                except:
                    stops[service_id]["platform"] = "NA"
                try:
                    stops[service_id]["route_before_arrival"] = ar.attrib.get("ppth")
                except:
                    stops[service_id]["route_before_arrival"] = "NA"

                try:
                    stops[service_id]["planned_arrival_time"] = parse_db_time(ar.attrib.get("pt"))
                except:
                    stops[service_id]["planned_arrival_time"] = "NA"

            except Exception as e:
                print(f"Arrival error. {e}")

        for dp in stop.findall("dp"):
            try:
                try:
                    stops[service_id]["route_after_departure"] = dp.attrib.get("ppth")
                except:
                    stops[service_id]["route_after_departure"] = "NA"
                try:
                    stops[service_id]["planned_departure_time"] = parse_db_time(dp.attrib.get("pt"))
                except:
                    stops[service_id]["planned_departure_time"] = "NA"
            except Exception as e:
                print(f"Departure error. {e}")
                continue
    return stops

def parse_recent_changes(response):
    root = ET.fromstring(response)
    stops = {}

    for stop in root.findall("s"):
        service_id = stop.attrib.get("id")
        stops[service_id] = {}

        for ar in stop.findall("ar"):
            try:
                try:
                    stops[service_id]["actual_arrival_time"] = ar.attrib.get("ct")
                except:
                    stops[service_id]["actual_arrival_time"] = "NA"
            except Exception as e:
                print(f"Arrival error. {e}")
        for dp in stop.findall("dp"):
            try:
                try:
                    stops[service_id]["actual_departure_time"] = dp.attrib.get("ct")
                except:
                    stops[service_id]["actual_departure_time"] = "NA"
            except Exception as e:
                print(f"Departure error. {e}")
                continue

# def fetch_states(conn):
#     try:
#         with conn.cursor() as cur:
#             cur.execute("SELECT DISTINCT federal_state FROM raw_stations;")
#             rows = cur.fetchall()
#             return rows
#     except Exception as e:
#         print(f"Error while fetching federal states. Error: {e}")
#     return None
