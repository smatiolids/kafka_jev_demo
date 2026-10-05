"""Sends the rows of data/dataset*.csv to a Kafka topic, one JSON message per row.

Usage: python send_message.py [--skip N] [--limit N] [--delay SECONDS]
"""

import argparse
import csv
import itertools
import json
import os
import sys
import time
import uuid
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

config = {"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]}
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

topic = os.getenv("KAFKA_TOPIC", "messages")
parser = argparse.ArgumentParser(description="Send the rows of data/dataset*.csv to Kafka as JSON.")
parser.add_argument("--skip", type=int, default=0, help="skip the first N rows (default: 0)")
parser.add_argument("--limit", type=int, default=None, help="send at most N rows (default: all)")
parser.add_argument("--delay", type=float, default=0, help="seconds to wait between messages")
args = parser.parse_args()

files = sorted(Path(__file__).parent.glob("data/dataset*.csv"))
if not files:
    sys.exit("No data/dataset*.csv files found")


def rows():
    for path in files:
        with path.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                # Drop empty columns, such as unused tag_N fields.
                yield {column: value for column, value in row.items() if value}


failed = 0


def on_delivery(error, message):
    global failed
    if error:
        failed += 1
        print(f"Delivery failed: {error}", file=sys.stderr)


producer = Producer(config)
sent = 0
stop = None if args.limit is None else args.skip + args.limit
for row in itertools.islice(rows(), args.skip, stop):
    producer.produce(topic, json.dumps(row, ensure_ascii=False), key=str(uuid.uuid4()), on_delivery=on_delivery)
    producer.poll(0)
    sent += 1
    if args.delay:
        time.sleep(args.delay)

if producer.flush(30):
    sys.exit("Timed out sending messages; check KAFKA_BOOTSTRAP_SERVERS and credentials")
print(f"Sent {sent - failed} of {sent} rows from {', '.join(f.name for f in files)} to {topic}")
if failed:
    sys.exit(1)
