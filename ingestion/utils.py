import xml.etree.ElementTree as ET

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

def parse_xml(response, recent_changes = False):

    root = ET.fromstring(response)
    records = []

    for stop in root.findall("s"):
        s_id = stop.attrib.get("id")

        # Get train info
        tl = stop.find("tl")
        train_id = tl.attrib.get("n") if tl is not None else None

        for ar in stop.findall("ar"):
            try:
                planned_path = ar.attrib.get("ppth")
                record = {
                    "s_id": s_id,
                    "train_id": train_id,
                    "planned_path": planned_path,
                    "type": "arrival",
                }
                if not recent_changes:
                    scheduled_time = ar.attrib.get("pt")
                    record["scheduled_time"] = scheduled_time

                else:
                    changed_time = ar.attrib.get("ct")
                    record["actual_time"] = changed_time
                records.append(record)
            except Exception as e:
                print(f"Arrival error. {e}")
                continue

        for dp in stop.findall("dp"):
            try:
                planned_path = dp.attrib.get("ppth")
                record = {
                    "s_id": s_id,
                    "train_id": train_id,
                    "planned_path": planned_path,
                    "type": "departure",
                }
                if not recent_changes:
                    scheduled_time = dp.attrib.get("pt")
                    record["scheduled_time"] = scheduled_time
                else:
                    changed_time = dp.attrib.get("ct")
                    record["actual_time"] = changed_time
                records.append(record)
            except Exception as e:
                print(f"Departure error. {e}")
                continue
    return records


# def fetch_states(conn):
#     try:
#         with conn.cursor() as cur:
#             cur.execute("SELECT DISTINCT federal_state FROM raw_stations;")
#             rows = cur.fetchall()
#             return rows
#     except Exception as e:
#         print(f"Error while fetching federal states. Error: {e}")
#     return None
