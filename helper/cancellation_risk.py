"""Reads a Jev output topic from the beginning and prints, as JSON, the tickets with cancellation risk.

Usage: python helper/cancellation_risk.py [--topic messages_jev] [--threshold 0.5] [--language pt]
                                  [--sort-by customer_dissatisfaction] [--no-text]
"""

import argparse
import json
import os
import sys
import uuid

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition
from dotenv import load_dotenv

load_dotenv()

parser = argparse.ArgumentParser(description="List the tickets in a Jev output topic that show cancellation risk.")
parser.add_argument("--topic", default=os.getenv("KAFKA_OUTPUT_TOPIC", "messages_jev"), help="topic to read")
parser.add_argument("--question", default="cancellation_risk", help="noul question to filter on (default: cancellation_risk)")
parser.add_argument(
    "--threshold",
    type=float,
    default=0.5,
    help="minimum noul value to count as at risk (default: 0.5)",
)
parser.add_argument("--language", help="keep only tickets in this language, such as pt (default: all)")
parser.add_argument(
    "--sort-by",
    help="question whose value orders the tickets, highest first (default: the --question)",
)
parser.add_argument("--no-text", action="store_true", help="leave the ticket text out of the output")
args = parser.parse_args()
sort_by = args.sort_by or args.question

config = {
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    # A throwaway group: partitions are assigned manually and offsets are never committed.
    "group.id": f"cancellation-risk-{uuid.uuid4()}",
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


def read_records(consumer):
    """Yields every record currently in the topic, then stops."""
    metadata = consumer.list_topics(args.topic, timeout=10).topics[args.topic]
    if metadata.error:
        sys.exit(f"Cannot read topic {args.topic}: {metadata.error}")

    end_offsets = {}
    for partition in metadata.partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(args.topic, partition), timeout=10)
        if high > low:
            end_offsets[partition] = high
    if not end_offsets:
        return

    consumer.assign([TopicPartition(args.topic, p, OFFSET_BEGINNING) for p in end_offsets])
    while end_offsets:
        message = consumer.poll(10)
        if message is None:
            sys.exit("Timed out reading the topic")
        if message.error():
            sys.exit(f"Read failed: {message.error()}")
        yield message
        if message.offset() + 1 >= end_offsets.get(message.partition(), 0):
            end_offsets.pop(message.partition(), None)


def answer_value(answer):
    """Returns the numeric value of an answer: its noul or score."""
    return answer.get("noul", answer.get("score", 0))


def compact(answer):
    """Keeps only the value of an answer and its confidence."""
    value = {key: answer[key] for key in ("choice", "score", "noul") if key in answer}
    if "confidence" in answer:
        value["confidence"] = answer["confidence"]
    return value


records = 0
at_risk = []

consumer = Consumer(config)
try:
    for message in read_records(consumer):
        try:
            record = json.loads(message.value())
            answers = record["jev"]["answers"]
        except (TypeError, ValueError, KeyError):
            continue

        records += 1
        risk = answers.get(args.question, {}).get("noul")
        if risk is None or risk < args.threshold:
            continue

        ticket_input = record.get("input", {})
        if args.language and ticket_input.get("language") != args.language:
            continue

        source = record.get("source", {})
        ticket = {
            "key": source.get("key"),
            "partition": source.get("partition"),
            "offset": source.get("offset"),
            "timestamp": source.get("timestamp"),
            args.question: risk,
            "answers": {question: compact(answer) for question, answer in answers.items()},
        }
        for field in ("subject", "language", "queue", "priority"):
            if field in ticket_input:
                ticket[field] = ticket_input[field]
        if not args.no_text:
            # Records sent as plain text carry it in "text"; dataset rows carry it in "body".
            ticket["body"] = ticket_input.get("body", ticket_input.get("text"))
        at_risk.append(ticket)
finally:
    consumer.close()

at_risk.sort(key=lambda t: answer_value(t["answers"].get(sort_by, {})), reverse=True)
print(
    json.dumps(
        {
            "topic": args.topic,
            "question": args.question,
            "threshold": args.threshold,
            "language": args.language,
            "sort_by": sort_by,
            "records": records,
            "at_risk": len(at_risk),
            "tickets": at_risk,
        },
        indent=2,
        ensure_ascii=False,
    )
)
