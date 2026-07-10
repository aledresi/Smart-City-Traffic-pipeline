
import csv
import os
import uuid
import random
from datetime import datetime

from fastapi import FastAPI
import uvicorn

app = FastAPI()

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
STREET_CSV_PATH = os.path.join(ROOT_DIR, "..", "streets_reference.csv")


def load_street_ids(csv_path):
    """Load up to 20 unique street IDs from the CSV."""
    street_ids = []
    seen = set()
    try:
        with open(csv_path, newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                street_id = (row.get("street_id") or "").strip()
                if not street_id or street_id in seen:
                    continue
                seen.add(street_id)
                street_ids.append(street_id)
                if len(street_ids) >= 20:
                    break
    except FileNotFoundError:
        pass

    if street_ids:
        return street_ids

    return [
        "AL-ZUBAYRI-ST",
        "GAMAL-ABDULNASSER-ST",
        "HADDA-ST",
        "AL-SITTEEN-ST",
        "AIRPORT-RD",
    ]


STREETS = load_street_ids(STREET_CSV_PATH)

CONGESTION_LEVELS = ["Clear", "Moderate", "Heavy", "Blocked"]


def _congestion_from_speed(speed_kph: int) -> str:
    """Derive a congestion label from average speed."""
    if speed_kph >= 60:
        return "Clear"
    elif speed_kph >= 35:
        return "Moderate"
    elif speed_kph >= 15:
        return "Heavy"
    else:
        return "Blocked"


@app.get("/telemetry")
async def get_telemetry():
    """Return a single telemetry snapshot for one street."""
    street_id = random.choice(STREETS)
    vehicle_count = random.randint(0, 200)
    avg_speed_kph = round(random.uniform(5.0, 100.0), 2)
    delay_minutes = round(random.uniform(0.0, 30.0), 2)
    congestion_level = _congestion_from_speed(int(avg_speed_kph))

    return {
        "telemetry_id": str(uuid.uuid4()),
        "street_id": street_id,
        "timestamp": datetime.utcnow().replace(microsecond=0).isoformat(),
        "vehicle_count": vehicle_count,
        "avg_speed_kph": avg_speed_kph,
        "delay_minutes": delay_minutes,
        "congestion_level": congestion_level,
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)