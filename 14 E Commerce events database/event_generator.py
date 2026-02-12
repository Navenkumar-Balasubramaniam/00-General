import argparse
import json
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# Faker is optional but useful if you later want richer data (cities, device types, etc.)
from faker import Faker

# Kafka client library (Confluent's client is fast and widely used)
try:
    from confluent_kafka import Producer
except ImportError:
    Producer = None
fake = Faker()

# -----------------------------
# 1) Small helpers: timestamps and IDs
# -----------------------------
def utc_now_iso() -> str:
    """
    Return current UTC timestamp in ISO-8601 format with milliseconds and 'Z' suffix.
    Example: 2026-02-11T13:05:21.123Z
    """
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def new_event_id() -> str:
    """Generate a unique ID for each event (used for deduping downstream)."""
    return str(uuid.uuid4())


def random_user_id() -> str:
    """
    Create a readable user id (not a UUID) so tables & dashboards are easier to demo.
    """
    return f"u_{random.randint(1000, 9999)}"


def new_session_id() -> str:
    """Generate a session id that groups events in a single visit."""
    return f"s_{uuid.uuid4().hex[:8]}"


def new_cart_id() -> str:
    """Generate a cart identifier (used in checkout_started)."""
    return f"c_{random.randint(100, 999)}"


def new_order_id() -> str:
    """Generate an order identifier (used in order_completed)."""
    return f"o_{random.randint(10000, 99999)}"


def new_payment_id() -> str:
    """Generate a payment identifier (used in order_completed)."""
    return f"pay_{random.randint(1000, 9999)}"
# -----------------------------
# 2) Product catalog (static MVP data)
# -----------------------------
@dataclass(frozen=True)
class Product:
    """
    A minimal product representation.
    In a real system, product metadata would come from a catalog service / DB.
    """
    product_id: str
    product_name: str
    category: str
    brand: str
    price_eur: float


# Tiny in-memory product list (enough to make demo metrics meaningful)
PRODUCTS: List[Product] = [
    Product("p_2001", "Running Shoes", "footwear", "ACME", 69.99),
    Product("p_2002", "Socks 3-Pack", "apparel", "ACME", 9.99),
    Product("p_2003", "Yoga Mat", "fitness", "ZenCo", 24.50),
    Product("p_2004", "Water Bottle", "fitness", "Hydra", 14.00),
    Product("p_2005", "Wireless Earbuds", "electronics", "Soundify", 49.99),
    Product("p_2006", "Phone Case", "electronics", "CaseLab", 12.99),
    Product("p_2007", "Backpack", "accessories", "TrailPro", 39.00),
    Product("p_2008", "Coffee Beans 1kg", "grocery", "Roastly", 18.75),
    Product("p_2009", "Hoodie", "apparel", "NorthPeak", 44.90),
    Product("p_2010", "Smartwatch Band", "electronics", "WristCo", 15.25),
]

# -----------------------------
# 3) Event envelope builder (shared schema for ALL events)
# -----------------------------
def build_envelope(
    event_type: str,
    user_id: str,
    session_id: str,
    source: str,
    payload: Dict[str, Any],
    schema_version: int = 1,
    event_ts: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build the standard event envelope. This is your "contract" for Spark downstream.

    Top-level fields:
    - schema_version: allow evolution later without breaking consumers
    - event_id:       unique event id (dedup key)
    - event_type:     determines how payload is interpreted
    - event_ts:       business/event time (used for windows, lateness, partitions)
    - user_id:        used to group and key in Kafka
    - session_id:     used for funnel analysis
    - source:         where event came from (web/app/payments)
    - payload:        event-specific data
    """
    return {
        "schema_version": schema_version,
        "event_id": new_event_id(),
        "event_type": event_type,
        "event_ts": event_ts or utc_now_iso(),
        "user_id": user_id,
        "session_id": session_id,
        "source": source,
        "payload": payload,
    }
# -----------------------------
# 4) Event creators (per event type)
# -----------------------------
def make_page_view(user_id: str, session_id: str, product: Product) -> Dict[str, Any]:
    """
    Simulate a user viewing a product page.
    """
    payload = {
        "page_url": f"/product/{product.product_id}",
        "referrer": random.choice(
            ["/", "/search?q=shoes", "/search?q=hoodie", "/category/fitness", "/category/electronics", None]
        ),
        "product_id": product.product_id,
    }
    # If referrer is None, omit it (makes JSON more realistic)
    payload = {k: v for k, v in payload.items() if v is not None}
    return build_envelope("page_view", user_id, session_id, "web", payload)


def make_add_to_cart(user_id: str, session_id: str, product: Product) -> Dict[str, Any]:
    """
    Simulate a user adding a product to cart.
    """
    qty = random.choice([1, 1, 1, 2])  # mostly 1
    payload = {
        "product_id": product.product_id,
        "quantity": qty,
        "unit_price": round(product.price_eur, 2),
        "currency": "EUR",
    }
    return build_envelope("add_to_cart", user_id, session_id, "web", payload)


def make_checkout_started(
    user_id: str, session_id: str, cart_id: str, items: List[Tuple[Product, int]]
) -> Dict[str, Any]:
    """
    Simulate checkout start. This typically contains the cart snapshot.
    """
    cart_value = sum(p.price_eur * q for p, q in items)
    payload = {
        "cart_id": cart_id,
        "cart_value": round(cart_value, 2),
        "currency": "EUR",
        "items": [
            {"product_id": p.product_id, "quantity": q, "unit_price": round(p.price_eur, 2)}
            for p, q in items
        ],
    }
    return build_envelope("checkout_started", user_id, session_id, "web", payload)


def make_order_completed(
    user_id: str, session_id: str, order_id: str, payment_id: str, items: List[Tuple[Product, int]]
) -> Dict[str, Any]:
    """
    Simulate a completed order event from a payments/checkout system.
    We compute a simple VAT and shipping to create realistic totals.
    """
    subtotal = sum(p.price_eur * q for p, q in items)
    tax = round(subtotal * 0.21, 2)  # simple VAT assumption for MVP
    shipping = 0.0 if subtotal >= 40 else 4.99
    total = round(subtotal + tax + shipping, 2)

    payload = {
        "order_id": order_id,
        "payment_id": payment_id,
        "total_amount": total,
        "currency": "EUR",
        "tax_amount": tax,
        "shipping_amount": round(shipping, 2),
        "items": [
            {"product_id": p.product_id, "quantity": q, "unit_price": round(p.price_eur, 2)}
            for p, q in items
        ],
        "payment_status": "captured",
    }
    return build_envelope("order_completed", user_id, session_id, "payments", payload)
# 5) Journey simulation logic (business behavior)
# -----------------------------
def choose_cart_items() -> List[Tuple[Product, int]]:
    """
    Pick 1-3 distinct products, each with quantity 1-2.
    This becomes the cart/order items list.
    """
    num_distinct = random.choice([1, 1, 2, 2, 3])
    chosen = random.sample(PRODUCTS, k=num_distinct)
    items: List[Tuple[Product, int]] = []
    for p in chosen:
        q = random.choice([1, 1, 2])
        items.append((p, q))
    return items


def journey_events(user_id: str) -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    Generate a *sequence* of events that represent one user session.

    Output: list of tuples (topic, key, event_json)
    - Kafka key:
      - user_activity events keyed by user_id (keeps ordering per user in partitions)
      - transaction events keyed by order_id (keeps ordering per order)
    """
    session_id = new_session_id()
    out: List[Tuple[str, str, Dict[str, Any]]] = []

    # Step A: Browsing - user views 1-4 products
    viewed = random.sample(PRODUCTS, k=random.choice([1, 2, 3, 4]))
    for p in viewed:
        out.append(("ecom.user_activity", user_id, make_page_view(user_id, session_id, p)))

    # Step B: Funnel probabilities (tune these to control conversion)
    # 55% chance the user adds something to cart
    if random.random() < 0.55:
        cart_items = choose_cart_items()

        # Add-to-cart events: one per product in cart
        for p, _q in cart_items:
            out.append(("ecom.user_activity", user_id, make_add_to_cart(user_id, session_id, p)))

        # 75% chance they start checkout
        if random.random() < 0.75:
            cart_id = new_cart_id()
            out.append(("ecom.user_activity", user_id, make_checkout_started(user_id, session_id, cart_id, cart_items)))

            # 60% chance they complete the purchase
            if random.random() < 0.60:
                order_id = new_order_id()
                payment_id = new_payment_id()
                out.append(
                    ("ecom.transactions", order_id,
                     make_order_completed(user_id, session_id, order_id, payment_id, cart_items))
                )

    return out
# 6) Kafka producer setup
# -----------------------------
def kafka_producer(bootstrap_servers: str) -> "Producer":
    """
    Create a Kafka producer with safe-ish defaults:
    - acks=all and idempotence -> reduces duplicates due to retries
    - small linger/batching -> better throughput without much latency
    """
    if Producer is None:
        raise RuntimeError("confluent-kafka is not installed. Run: pip install confluent-kafka")

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "ecom-synth-generator",
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
        "batch.num.messages": 1000,
    }
    return Producer(conf)


def delivery_report(err, msg):
    """
    Called by confluent-kafka when a produce request succeeds/fails.
    For MVP we print only errors to keep output clean.
    """
    if err is not None:
        print(f"[DELIVERY ERROR] {err}")
# -----------------------------
# 7) Main run loop (generate + send events continuously)
# -----------------------------
def run(
    bootstrap_servers: str,
    rate_per_sec: float,
    duration_sec: Optional[int],
    dry_run: bool,
    seed: Optional[int],
):
    """
    Generate events forever or for a fixed duration.

    rate_per_sec = approx journeys per second
    - Each journey produces multiple events (page views + funnel events),
      so actual events/sec will be higher than rate_per_sec.
    """
    # Seed randomness for reproducible demos
    if seed is not None:
        random.seed(seed)
        Faker.seed(seed)

    # Create Kafka producer unless dry-run is enabled
    producer = None if dry_run else kafka_producer(bootstrap_servers)

    # Sleep interval between journeys
    interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0

    start = time.time()
    sent = 0

    try:
        while True:
            # Stop if duration is set and reached
            if duration_sec is not None and (time.time() - start) >= duration_sec:
                break

            # Generate one user journey worth of events
            user_id = random_user_id()
            events = journey_events(user_id)

            # Send each event to its Kafka topic
            for topic, key, ev in events:
                payload = json.dumps(ev).encode("utf-8")

                if dry_run:
                    # Print a tab-separated line for quick scanning
                    print(f"{topic}\tkey={key}\t{json.dumps(ev)}")
                else:
                    # Produce to Kafka with key so ordering is preserved per key within a partition
                    producer.produce(
                        topic=topic,
                        key=str(key).encode("utf-8"),
                        value=payload,
                        callback=delivery_report,
                    )
                sent += 1

            if not dry_run:
                # Serve delivery callbacks and internal producer events
                producer.poll(0)

            # Control the journey rate
            if interval > 0:
                time.sleep(interval)

    except KeyboardInterrupt:
        # Allow clean CTRL+C shutdown
        pass
    finally:
        # Flush ensures buffered messages are delivered before exit
        if not dry_run and producer is not None:
            producer.flush(10)

    print(f"Done. Sent {sent} events.")
# -----------------------------
# 8) CLI entrypoint
# -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic e-commerce event generator (Kafka).")

    # Your VM likely exposes Kafka at <VM_IP>:9092 or via internal DNS
    parser.add_argument(
        "--bootstrap",
        default="localhost:9092",
        help="Kafka bootstrap servers, e.g. 10.0.0.12:9092",
    )

    # Rate = journeys/sec (not events/sec)
    parser.add_argument(
        "--rate",
        type=float,
        default=2.0,
        help="Journeys per second (approx). Each journey emits multiple events.",
    )

    # Duration = seconds; use 0 to run continuously
    parser.add_argument(
        "--duration",
        type=int,
        default=30,
        help="Run duration in seconds. Use 0 for infinite.",
    )

    # Dry-run prints instead of producing to Kafka
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print events instead of sending to Kafka.",
    )

    # Seed for reproducible event streams
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducibility.",
    )

    args = parser.parse_args()

    run(
        bootstrap_servers=args.bootstrap,
        rate_per_sec=max(args.rate, 0.0001),
        duration_sec=None if args.duration == 0 else args.duration,
        dry_run=args.dry_run,
        seed=args.seed,
    )
