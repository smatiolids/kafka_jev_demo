"""Deletes every record from Kafka topics whose names start with ``messages``.

Usage: python helper/clear_messages.py --yes
"""

import argparse
import os
import re
import sys

from confluent_kafka import OFFSET_END, TopicPartition
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv

load_dotenv()

parser = argparse.ArgumentParser(description="Clear records from Kafka topics matching a name pattern.")
parser.add_argument(
    "--pattern",
    default=r"^messages.*",
    help="topic-name regex (default: ^messages.*)",
)
parser.add_argument("--yes", action="store_true", help="confirm permanent record deletion")
args = parser.parse_args()

try:
    topic_pattern = re.compile(args.pattern)
except re.error as error:
    parser.error(f"invalid --pattern: {error}")

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

admin = AdminClient(config)
metadata = admin.list_topics(timeout=10)
topics = sorted(name for name, topic in metadata.topics.items() if topic_pattern.match(name) and not topic.error)

if not topics:
    sys.exit(f"No topics match {args.pattern}")

print("Topics to clear:")
for topic in topics:
    print(f"  {topic}")

if not args.yes:
    print("No records deleted. Run again with --yes to confirm.")
    sys.exit(0)

partitions = [
    TopicPartition(topic, partition, OFFSET_END)
    for topic in topics
    for partition in metadata.topics[topic].partitions
]
futures = admin.delete_records(partitions, request_timeout=30, operation_timeout=30)

failed = []
for topic_partition, future in futures.items():
    try:
        future.result()
    except Exception as error:  # confluent-kafka futures surface broker-specific exceptions.
        failed.append((topic_partition, error))

if failed:
    for topic_partition, error in failed:
        print(f"Failed to clear {topic_partition.topic}[{topic_partition.partition}]: {error}", file=sys.stderr)
    sys.exit(1)

print(f"Cleared {len(topics)} topic(s) across {len(partitions)} partition(s).")
