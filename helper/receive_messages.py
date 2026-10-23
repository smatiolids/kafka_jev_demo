"""Receives and prints records from the routed Jev output topics.

Usage: python helper/receive_messages.py [--topic TOPIC] [--from-beginning] [--limit N]
"""

import argparse
import json
import os
import sys
import uuid

from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

configured_topics = os.getenv("KAFKA_OUTPUT_TOPICS") or os.getenv("KAFKA_OUTPUT_TOPIC")
default_topics = [topic.strip() for topic in configured_topics.split(",")] if configured_topics else None

parser = argparse.ArgumentParser(description="Receive records from the routed Jev output topics.")
parser.add_argument(
    "--topic",
    action="append",
    dest="topics",
    help="exact topic to read; repeat for multiple topics (overrides --pattern)",
)
parser.add_argument(
    "--pattern",
    default=os.getenv("KAFKA_OUTPUT_TOPIC_PATTERN", r"^messages_jev.*"),
    help="Kafka topic regex (default: ^messages_jev.*)",
)
parser.add_argument(
    "--from-beginning",
    action="store_true",
    help="include records already in the topic (default: only new records)",
)
parser.add_argument("--limit", type=int, help="stop after receiving N records")
parser.add_argument(
    "--timeout",
    type=float,
    help="stop if no record arrives for this many seconds (default: wait forever)",
)
parser.add_argument("--pretty", action="store_true", help="pretty-print JSON across multiple lines")
parser.add_argument(
    "--color",
    choices=("auto", "always", "never"),
    default="auto",
    help="highlight topic names (default: auto)",
)
args = parser.parse_args()
topics = args.topics or default_topics
subscription = topics or [args.pattern]

if args.limit is not None and args.limit < 1:
    parser.error("--limit must be at least 1")
if args.timeout is not None and args.timeout <= 0:
    parser.error("--timeout must be greater than 0")

config = {
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    # Each invocation is independent, so --from-beginning behaves predictably.
    "group.id": f"receive-messages-{uuid.uuid4()}",
    "auto.offset.reset": "earliest" if args.from_beginning else "latest",
    "enable.auto.commit": False,
}
if os.getenv("KAFKA_API_KEY"):
    config.update(
        {
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": os.environ["KAFKA_API_KEY"],
            "sasl.password": os.environ["KAFKA_API_SECRET"],
        }
    )

if os.getenv("KAFKA_SSL_CA_LOCATION"):
    config["ssl.ca.location"] = os.environ["KAFKA_SSL_CA_LOCATION"]


def decode_value(value):
    """Decode a Kafka value, preserving non-JSON payloads as text."""
    if value is None:
        return None
    text = value.decode("utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def topic_header(topic):
    """Return a visible topic label, colored when output supports it."""
    use_color = args.color == "always" or (args.color == "auto" and sys.stdout.isatty() and not os.getenv("NO_COLOR"))
    label = f"[{topic}]"
    return f"\033[1;36m{label}\033[0m" if use_color else label


consumer = Consumer(config)
received = 0

try:
    consumer.subscribe(subscription)
    source = ", ".join(topics) if topics else f"topics matching {args.pattern}"
    print(f"Waiting for records from {source}. Press Ctrl-C to stop.", file=sys.stderr)

    while args.limit is None or received < args.limit:
        message = consumer.poll(args.timeout if args.timeout is not None else 1.0)
        if message is None:
            if args.timeout is not None:
                break
            continue
        if message.error():
            sys.exit(f"Read failed: {message.error()}")

        value = decode_value(message.value())
        print(topic_header(message.topic()))
        if args.pretty:
            print(json.dumps(value, ensure_ascii=False, indent=2))
        else:
            print(json.dumps(value, ensure_ascii=False, separators=(",", ":")))
        received += 1
except KeyboardInterrupt:
    pass
finally:
    consumer.close()

print(f"Received {received} record(s).", file=sys.stderr)
