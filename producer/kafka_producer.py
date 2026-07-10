
import json
import os
import subprocess
import sys
import time
import requests
import pandas as pd
import psycopg2
from confluent_kafka import Producer

conf     = {"bootstrap.servers": "localhost:9093"}
producer = Producer(conf)
last_sent_incident_id = ""   

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
STREET_CSV_PATH = os.path.join(ROOT_DIR, "streets_reference.csv")
PYTHON_EXE = sys.executable
street_catalog = []
street_index = 0


def start_data_generators():
    """Launch the incident and sensor generators in the background."""
    scripts = [
        ("incident", [PYTHON_EXE, os.path.join(ROOT_DIR, "data_generators", "incident_gen.py")]),
        ("sensor", [PYTHON_EXE, os.path.join(ROOT_DIR, "data_generators", "sensor_api.py")]),
    ]

    for name, cmd in scripts:
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=ROOT_DIR,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            print(f"[GEN] Started {name} (pid={proc.pid})")
        except Exception as exc:
            print(f"[GEN] Failed to start {name}: {exc}")


start_data_generators()


# ── Delivery Callback ─────────────────────────────────────────────────────────
def delivery_report(err, msg):
    if err is not None:
        print(f"[KAFKA] ❌ Delivery failed: {err}")
    else:
        print(f"[KAFKA] ✅ Delivered → {msg.topic()} [partition {msg.partition()}]")


# ── Source 1: PostgreSQL – traffic_incidents ──────────────────────────────────
def get_incident_data():
    """
    Fetch new incidents from PostgreSQL (incident_id is a UUID string).
    Returns the newest undelivered record, or None.
    """
    global last_sent_incident_id
    try:
        conn = psycopg2.connect(
            "dbname=traffic_data user=postgres password=password "
            "host=localhost port=5433"
        )
        if last_sent_incident_id:
            query = """
                SELECT incident_id, street_id, incident_type, severity,
                       latitude, longitude, status, description, created_at
                FROM   traffic_incidents
                WHERE  created_at > (
                    SELECT created_at FROM traffic_incidents
                    WHERE  incident_id = %s
                )
                ORDER BY created_at ASC
                LIMIT 1
            """
            df = pd.read_sql(query, conn, params=(last_sent_incident_id,))
        else:
            query = """
                SELECT incident_id, street_id, incident_type, severity,
                       latitude, longitude, status, description, created_at
                FROM   traffic_incidents
                ORDER BY created_at ASC
                LIMIT 1
            """
            df = pd.read_sql(query, conn)

        conn.close()

        if not df.empty:
            record = df.to_dict(orient="records")[0]
            last_sent_incident_id = record["incident_id"]
            return record

    except Exception as e:
        print(f"[DB] ❌ Error: {e}")
    return None


# ── Source 2: CSV – streets_reference.csv ────────────────────────────────────
def load_street_catalog():
    """Load the street reference catalog once from the CSV file."""
    global street_catalog, street_index
    try:
        df = pd.read_csv(STREET_CSV_PATH)
        if df.empty:
            street_catalog = []
        else:
            street_catalog = df.to_dict(orient="records")
        street_index = 0
        print(f"[CSV] Loaded {len(street_catalog)} street reference records.")
    except Exception as e:
        print(f"[CSV] ❌ Error loading street catalog: {e}")
        street_catalog = []
        street_index = 0


load_street_catalog()


def get_street_data():
    """Return the next street record from the static reference CSV once."""
    global street_index
    if street_index >= len(street_catalog):
        return None
    street = street_catalog[street_index]
    street_index += 1
    return street


# ── Source 3: FastAPI – /telemetry ────────────────────────────────────────────
def get_telemetry_data():
    """
    Call the sensor FastAPI and return the telemetry payload.
    Keys: telemetry_id, street_id, timestamp, vehicle_count,
          avg_speed_kph, delay_minutes, congestion_level
    """
    try:
        response = requests.get("http://localhost:5000/telemetry", timeout=5)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"[API] ❌ Connection error: {e}")
    return None


# ── Main Loop 
while True:
    print("\n─── Polling sources ─────────────────────────────────────────")

    # 1. Incidents (PostgreSQL → incidents_topic)
    incident = get_incident_data()
    if incident:
        producer.produce(
            "incidents_topic",
            json.dumps(incident, default=str),
            callback=delivery_report,
        )
        print(f"[DB]  → incidents_topic | {incident['incident_type']} on {incident['street_id']}")
    else:
        print("[DB]  No new incidents found.")

    # 2. Street reference (CSV → streets_topic)
    street = get_street_data()
    if street:
        producer.produce(
            "streets_topic",
            json.dumps(street, default=str),
            callback=delivery_report,
        )
        print(f"[CSV] → streets_topic | {street['street_id']} | zone={street['zone_id']}")
    else:
        print("[CSV] No street data found.")

    # 3. Telemetry (FastAPI → telemetry_topic)
    telemetry = get_telemetry_data()
    if telemetry:
        producer.produce(
            "telemetry_topic",
            json.dumps(telemetry, default=str),
            callback=delivery_report,
        )
        print(
            f"[API] → telemetry_topic | {telemetry['street_id']} | "
            f"speed={telemetry['avg_speed_kph']} kph | "
            f"congestion={telemetry['congestion_level']}"
        )
    else:
        print("[API] No telemetry data returned.")

    producer.flush()
    time.sleep(10)