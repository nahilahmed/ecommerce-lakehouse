"""
Clickstream Producer — ShopMetrics Inc.
FR-010: Synthetic clickstream events published to Confluent Kafka at ~1 event/sec.

HOW IT WORKS
------------
This script simulates users browsing the ShopMetrics store. It models realistic
session behaviour:
  - A "session" is one user's continuous visit (a sequence of events)
  - Each session has a customer, a session ID, and follows a realistic page flow
  - Sessions end naturally after a random number of events, then a new one starts
  - Events are published to Kafka as JSON messages at ~1 event/sec

WHY JSON?
---------
Kafka messages are raw bytes. JSON is the standard format for semi-structured
event data — easy to produce here and easy to parse in Spark downstream.

CREDENTIALS
-----------
Never hardcode credentials. This script reads from a local .env file (gitignored).
Copy .env.example to .env and fill in your Confluent values.

Usage:
    pip install confluent-kafka python-dotenv
    python produce_clickstream.py              # runs until Ctrl+C
    python produce_clickstream.py --events 100 # stops after 100 events
"""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()  # reads .env file into os.environ

TOPIC = "clickstream-events"

# ~1 event/sec — slight jitter (0.8–1.2s) makes it feel more realistic
MIN_DELAY = 0.8
MAX_DELAY = 1.2

# Event types from BRD §7 clickstream_raw schema
EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "search"]

# Realistic page paths a user might visit
PAGES = [
    "/",
    "/category/electronics",
    "/category/clothing",
    "/category/books",
    "/category/home-garden",
    "/category/sports",
    "/category/toys",
    "/category/beauty",
    "/category/food",
    "/search",
    "/cart",
    "/checkout",
]

# Pool sizes matching your generated data
# (10K customers, 1K products from data-generator/)
CUSTOMER_ID_RANGE = (1, 10_000)
PRODUCT_ID_RANGE = (1, 1_000)

# Session length: how many events before starting a new session
SESSION_MIN_EVENTS = 3
SESSION_MAX_EVENTS = 15


# ---------------------------------------------------------------------------
# Session simulator
# ---------------------------------------------------------------------------

class Session:
    """
    Represents one user's visit to the store.

    A session starts when a user lands on the site and ends after inactivity
    (modelled here as a fixed event count). The 30-min inactivity window for
    sessionization happens downstream in Silver, not here.
    """

    def __init__(self):
        self.session_id = str(uuid.uuid4())
        self.customer_id = random.randint(*CUSTOMER_ID_RANGE)
        self.events_remaining = random.randint(SESSION_MIN_EVENTS, SESSION_MAX_EVENTS)
        # Track state so event flow feels realistic
        self._has_added_to_cart = False

    def is_active(self) -> bool:
        return self.events_remaining > 0

    def next_event_type(self) -> str:
        """
        Weight event types based on session state.
        Real users mostly page_view, occasionally search/add_to_cart, rarely purchase.
        """
        if self._has_added_to_cart and self.events_remaining == 1:
            # Last event in session after adding to cart — simulate checkout
            return "purchase"
        elif self._has_added_to_cart:
            return random.choices(
                ["page_view", "add_to_cart", "search"],
                weights=[60, 20, 20]
            )[0]
        else:
            return random.choices(
                EVENT_TYPES,
                weights=[65, 15, 5, 15]  # page_view dominates
            )[0]

    def next_page(self, event_type: str) -> str:
        if event_type == "search":
            return "/search"
        elif event_type in ("add_to_cart", "purchase"):
            return "/cart"
        else:
            return random.choice(PAGES)

    def generate_event(self) -> dict:
        event_type = self.next_event_type()

        if event_type == "add_to_cart":
            self._has_added_to_cart = True

        event = {
            # Unique ID for this event — UUID4 is random, collision-proof
            "event_id": str(uuid.uuid4()),
            "customer_id": self.customer_id,
            # Not every event is product-specific (e.g. home page visit)
            "product_id": random.randint(*PRODUCT_ID_RANGE) if event_type != "search" else None,
            "event_type": event_type,
            # session_id ties all events in this visit together
            "session_id": self.session_id,
            "page": self.next_page(event_type),
            # ISO 8601 timestamp in UTC — Spark will parse this downstream
            "event_ts": datetime.now(timezone.utc).isoformat(),
        }

        self.events_remaining -= 1
        return event


# ---------------------------------------------------------------------------
# Kafka producer setup
# ---------------------------------------------------------------------------

def build_producer() -> Producer:
    """
    Creates a Confluent Kafka producer configured for SASL_SSL auth.

    SASL_SSL = username/password auth over a TLS-encrypted connection.
    This is what Confluent Cloud requires for all external clients.
    """
    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVER"]
    api_key = os.environ["KAFKA_API_KEY"]
    api_secret = os.environ["KAFKA_API_SECRET"]

    config = {
        "bootstrap.servers": bootstrap,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": api_key,
        "sasl.password": api_secret,
        # How long to wait before giving up on connecting (ms)
        "socket.timeout.ms": 10_000,
    }

    return Producer(config)


def delivery_callback(err, msg):
    """
    Called by Kafka after each message is acknowledged by the broker.

    WHY A CALLBACK?
    Kafka producers are asynchronous — produce() returns immediately without
    waiting for the broker to confirm. The callback fires later when the broker
    responds. This is how Kafka achieves high throughput.

    err=None means the broker confirmed the message was written to a partition.
    """
    if err:
        print(f"  ❌ Delivery failed: {err}")
    else:
        print(
            f"  ✅ [{msg.topic()}] partition={msg.partition()} "
            f"offset={msg.offset()} key={msg.key()}"
        )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(max_events: int | None = None):
    producer = build_producer()
    session = Session()

    total = 0
    print(f"Publishing to '{TOPIC}' — Ctrl+C to stop\n")

    try:
        while max_events is None or total < max_events:
            # Start a fresh session when the current one ends
            if not session.is_active():
                session = Session()
                print(f"\n→ New session: customer={session.customer_id} "
                      f"session_id={session.session_id[:8]}...")

            event = session.generate_event()
            payload = json.dumps(event).encode("utf-8")

            # KEY: using customer_id as the Kafka message key.
            # WHY? Kafka routes messages with the same key to the same partition.
            # This guarantees all events for one customer arrive in order,
            # which matters when sessionizing downstream in Silver.
            key = str(event["customer_id"]).encode("utf-8")

            producer.produce(
                topic=TOPIC,
                key=key,
                value=payload,
                on_delivery=delivery_callback,
            )

            # poll() triggers delivery callbacks for any pending messages.
            # Without this, callbacks never fire in a single-threaded script.
            producer.poll(0)

            total += 1
            delay = random.uniform(MIN_DELAY, MAX_DELAY)
            time.sleep(delay)

    except KeyboardInterrupt:
        print(f"\nStopped. Flushing remaining messages...")

    finally:
        # flush() blocks until all buffered messages are delivered or time out.
        # Always call this before exiting — otherwise buffered events are lost.
        producer.flush(timeout=10)
        print(f"Done. Total events published: {total}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ShopMetrics clickstream producer")
    parser.add_argument(
        "--events",
        type=int,
        default=None,
        help="Stop after N events (default: run indefinitely)",
    )
    args = parser.parse_args()
    run(max_events=args.events)
