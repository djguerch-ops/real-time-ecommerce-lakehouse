"""
Streaming event producer — V2.

Same event generation logic as src/event_generator/producer.py, but
instead of writing a batch file, each event is sent individually to
a Kinesis Data Stream as soon as it's generated. This simulates a
real e-commerce site emitting events in real time.
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

import boto3

STREAM_NAME = "rtl-dev-events-stream"
AWS_REGION = "eu-west-1"

EVENT_TYPES = ["page_view", "add_to_cart", "order_created"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
COUNTRIES = ["FR", "DE", "ES", "IT"]
PRODUCT_IDS = [f"p_{i:03d}" for i in range(1, 21)]

kinesis = boto3.client("kinesis", region_name=AWS_REGION)


def generate_event() -> dict:
    event_type = random.choices(EVENT_TYPES, weights=[0.7, 0.2, 0.1], k=1)[0]

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
        "currency": "EUR",
    }

    if event_type == "order_created":
        event["order_id"] = f"o_{random.randint(100000, 999999)}"
        event["amount"] = round(random.uniform(10, 500), 2)

    return event


def send_event(event: dict) -> None:
    """
    Sends one event to Kinesis. The partition key determines which
    shard the record lands on — using user_id keeps all events from
    the same user in order on the same shard.
    """
    kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(event).encode("utf-8"),
        PartitionKey=event["user_id"],
    )


def run(n_events: int = 100, delay_seconds: float = 0.2) -> None:
    """
    Generates and sends events one at a time with a small delay,
    simulating a live traffic stream rather than a batch dump.
    """
    for i in range(n_events):
        event = generate_event()
        send_event(event)
        print(f"[{i + 1}/{n_events}] sent {event['event_type']} ({event['event_id'][:8]}...)")
        time.sleep(delay_seconds)

    print(f"Done — {n_events} events streamed to {STREAM_NAME}")


if __name__ == "__main__":
    run(n_events=100, delay_seconds=0.2)
