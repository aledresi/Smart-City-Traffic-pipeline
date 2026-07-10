
import csv
import json
import os
import time
import uuid
import random
from datetime import datetime

import psycopg2

# ── Config ────────────────────────────────────────────────────────────────────
DB_CONFIG = (
    "dbname=traffic_data user=postgres password=password "
    "host=localhost port=5433"
)
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
STREET_CSV_PATH = os.path.join(ROOT_DIR, "..", "streets_reference.csv")


def load_street_catalog(csv_path):
    """Load up to 20 unique streets and their coordinates from the CSV."""
    streets = {}
    seen = set()
    try:
        with open(csv_path, newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                street_id = (row.get("street_id") or "").strip()
                if not street_id or street_id in seen:
                    continue
                seen.add(street_id)
                geometry = json.loads((row.get("geometry_json") or "{}") or "{}")
                coordinates = geometry.get("coordinates", [])
                if coordinates and isinstance(coordinates[0], list) and len(coordinates[0]) >= 2:
                    lon = float(coordinates[0][0])
                    lat = float(coordinates[0][1])
                else:
                    lat, lon = 15.37, 44.20
                streets[street_id] = (lat, lon)
                if len(streets) >= 20:
                    break
    except FileNotFoundError:
        pass

    if streets:
        return streets

    return {
        "AL-ZUBAYRI-ST": (15.3560, 44.2066),
        "GAMAL-ABDULNASSER-ST": (15.3694, 44.1910),
        "HADDA-ST": (15.3432, 44.1989),
        "AL-SITTEEN-ST": (15.3774, 44.2085),
        "AIRPORT-RD": (15.4792, 44.2194),
    }


STREETS = load_street_catalog(STREET_CSV_PATH)

INCIDENT_TYPES = ["Accident", "Roadwork", "Flood", "Vehicle Breakdown", "Protest"]
STATUSES       = ["Active", "Under Investigation", "Cleared"]

DESCRIPTIONS = {
    "Accident":           "Multi-vehicle collision blocking lanes.",
    "Roadwork":           "Scheduled maintenance reducing lane capacity.",
    "Flood":              "Water accumulation making road impassable.",
    "Vehicle Breakdown":  "Stalled vehicle on carriageway.",
    "Protest":            "Public gathering causing road closure.",
}


# ── DB Init ───────────────────────────────────────────────────────────────────
def ensure_table(cur, conn):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS traffic_incidents (
            incident_id  VARCHAR(36) PRIMARY KEY,
            street_id    VARCHAR(100),
            incident_type VARCHAR(50),
            severity     INT,
            latitude     DECIMAL(9,6),
            longitude    DECIMAL(9,6),
            status       VARCHAR(30),
            description  TEXT,
            created_at   TIMESTAMP
        );
    """)
    conn.commit()


# ── Generator ─────────────────────────────────────────────────────────────────
def run_incident_generator():
    conn = psycopg2.connect(DB_CONFIG)
    cur  = conn.cursor()
    ensure_table(cur, conn)

    while True:
        street_id = random.choice(list(STREETS.keys()))
        base_lat, base_lon = STREETS[street_id]

        # Add a tiny random offset so incidents aren't all at the exact centre
        latitude  = round(base_lat + random.uniform(-0.002, 0.002), 6)
        longitude = round(base_lon + random.uniform(-0.002, 0.002), 6)

        incident_type = random.choice(INCIDENT_TYPES)
        severity      = random.randint(1, 5)
        status        = random.choice(STATUSES)
        description   = DESCRIPTIONS[incident_type]
        incident_id   = str(uuid.uuid4())
        created_at    = datetime.utcnow().replace(microsecond=0).isoformat()

        cur.execute(
            """
            INSERT INTO traffic_incidents
              (incident_id, street_id, incident_type, severity,
               latitude, longitude, status, description, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (incident_id, street_id, incident_type, severity,
             latitude, longitude, status, description, created_at),
        )
        conn.commit()
        print(f"[INCIDENT] {incident_type} on {street_id} | sev={severity} | {created_at}")
        time.sleep(10)


if __name__ == "__main__":
    run_incident_generator()