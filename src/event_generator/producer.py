import json
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path

EVENT_TYPES = ["page_view", "add_to_cart", "order_created"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
COUNTRIES = ["FR", "DE", "ES", "IT"]
PRODUCT_IDS = [f"p_{i:03d}" for i in range(1, 21)]


def generate_event() -> dict:
    event_type = random.choices(
        EVENT_TYPES,
        weights=[0.7, 0.2, 0.1],
        k=1
    )[0]

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "user_id": f"u_{random.randint(1000, 9999)}",
        "session_id": f"s_{random.randint(10000, 99999)}",
        "product_id": random.choice(PRODUCT_IDS),
        "order_id": None,
        "device_type": random.choice(DEVICE_TYPES),
        "country": random.choice(COUNTRIES),
        "amount": None,
        "currency": "EUR"
    }

    if event_type == "order_created":
        event["order_id"] = f"o_{random.randint(100000, 999999)}"
        event["amount"] = round(random.uniform(10, 500), 2)

    return event


def generate_events_file(output_path: str, n: int = 1000) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as f:
        for _ in range(n):
            f.write(json.dumps(generate_event()) + "\n")


if __name__ == "__main__":
    generate_events_file("data/sample/events.json", n=1000)
    print("Fichier genere : data/sample/events.json")
