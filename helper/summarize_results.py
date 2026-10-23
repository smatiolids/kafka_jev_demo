"""Reads Jev output topics from the beginning and summarizes their records.

Usage: python helper/summarize_results.py [--pattern '^messages_jev.*']
"""

import argparse
import json
import os
import re
import statistics
import sys
import uuid
from collections import Counter, defaultdict
from datetime import datetime

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition
from dotenv import load_dotenv

load_dotenv()

parser = argparse.ArgumentParser(description="Summarize Enriched Records across Jev output topics.")
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
    "--count-topic",
    action="append",
    dest="count_topics",
    help="additional topic to count without summarizing; repeat for multiple topics (default: messages)",
)
parser.add_argument(
    "--input-price",
    type=float,
    default=0.042,
    help="USD per million input tokens (default: 0.042, the Jev decision model price; output tokens are free)",
)
args = parser.parse_args()
configured_count_topics = os.getenv("KAFKA_COUNT_TOPICS", "messages")
default_count_topics = [topic.strip() for topic in configured_count_topics.split(",") if topic.strip()]

TOPIC_GROUPS = [
    ("Input", {"messages"}),
    ("Fixed", {"messages_jev"}),
    (
        "Choice",
        {
            "messages_jev_billing",
            "messages_jev_technical_support",
            "messages_jev_account_security",
            "messages_jev_retention",
            "messages_jev_general_support",
        },
    ),
    ("Noul", {"messages_jev_to_human", "messages_jev_to_agent"}),
    (
        "Score level",
        {
            "messages_jev_dissatisfaction_0",
            "messages_jev_dissatisfaction_1",
            "messages_jev_dissatisfaction_2",
            "messages_jev_dissatisfaction_3",
        },
    ),
    ("Score value", {"messages_jev_urgent", "messages_jev_normal"}),
    ("Dead letter", {"messages_jev_dlq"}),
]

try:
    topic_pattern = re.compile(args.pattern)
except re.error as error:
    parser.error(f"invalid --pattern: {error}")

config = {
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    # A throwaway group: partitions are assigned manually and offsets are never committed.
    "group.id": f"summarize-results-{uuid.uuid4()}",
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


def parse_time(value):
    """Parses an ISO-8601 UTC timestamp, truncating nanoseconds to microseconds."""
    value = value.rstrip("Z")
    if "." in value:
        whole, fraction = value.split(".")
        value = f"{whole}.{fraction[:6]}"
    return datetime.fromisoformat(value)


def discover_topics(consumer):
    """Return summary topics plus any additional count-only topics."""
    metadata = consumer.list_topics(timeout=10).topics
    summary_topics = args.topics or sorted(name for name in metadata if topic_pattern.match(name))
    if not summary_topics:
        sys.exit(f"No topics match {args.pattern}")
    count_topics = args.count_topics or default_count_topics
    topics = sorted(set(summary_topics) | set(count_topics))
    for topic in topics:
        if topic not in metadata:
            sys.exit(f"Topic does not exist: {topic}")
        if metadata[topic].error:
            sys.exit(f"Cannot read topic {topic}: {metadata[topic].error}")
    return topics, set(summary_topics), metadata


def read_records(consumer, topics, metadata):
    """Yields every record currently in the selected topics, then stops."""
    end_offsets = {}
    for topic in topics:
        for partition in metadata[topic].partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
            if high > low:
                end_offsets[(topic, partition)] = high
    if not end_offsets:
        return

    consumer.assign([TopicPartition(topic, partition, OFFSET_BEGINNING) for topic, partition in end_offsets])
    while end_offsets:
        message = consumer.poll(10)
        if message is None:
            sys.exit("Timed out reading the topic")
        if message.error():
            sys.exit(f"Read failed: {message.error()}")
        yield message
        position = (message.topic(), message.partition())
        if message.offset() + 1 >= end_offsets.get(position, 0):
            end_offsets.pop(position, None)


def percentile(values, p):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(p / 100 * (len(ordered) - 1)))]


LATENCY_STATS = {
    "avg": statistics.mean,
    "min": min,
    "p50": lambda v: percentile(v, 50),
    "p95": lambda v: percentile(v, 95),
    "p99": lambda v: percentile(v, 99),
    "max": max,
}


def print_latency_table(columns):
    """Prints one row per statistic and one column per latency series."""
    print("  " + " " * 8 + "".join(f"{label:>14}" for label in columns))
    for stat, compute in LATENCY_STATS.items():
        cells = "".join(f"{compute(v):>14,.0f}" if v else f"{'-':>14}" for v in columns.values())
        print(f"  {stat:<8}{cells}")


LABEL_WIDTH = 40


def bar(count, total, width=30):
    share = count / total
    return f"{count:>6}  {share:>6.1%}  {'#' * round(share * width)}"


def print_choice(items):
    choices = Counter(a["choice"] for a in items if "choice" in a)
    # Options never chosen still appear in the probabilities, so list them with 0.
    for answer in items:
        for option in answer.get("probabilities", {}):
            choices.setdefault(option, 0)
    confidences = [a["confidence"] for a in items if "confidence" in a]
    if confidences:
        print(f"    avg confidence {statistics.mean(confidences):.2f}")
    for choice, count in choices.most_common():
        print(f"    {choice:<{LABEL_WIDTH}} {bar(count, len(items))}")


def print_score(items):
    scores = [a["score"] for a in items if "score" in a]
    confidences = [a["confidence"] for a in items if "confidence" in a]
    legend = next((a["legend"] for a in items if "legend" in a), {})
    if not scores:
        return
    line = f"    avg score {statistics.mean(scores):.2f}"
    if confidences:
        line += f", avg confidence {statistics.mean(confidences):.2f}"
    print(line + "  (scores rounded to the nearest level)")
    levels = Counter(round(score) for score in scores)
    for level in sorted(set(levels) | {int(k) for k in legend}):
        label = f"{level} {legend.get(str(level), '')}".strip()
        print(f"    {label[:LABEL_WIDTH]:<{LABEL_WIDTH}} {bar(levels[level], len(scores))}")


NOUL_BUCKETS = [(0, 0.25), (0.25, 0.5), (0.5, 0.75), (0.75, 1.0001)]


def print_noul(items):
    values = [a["noul"] for a in items if "noul" in a]
    if not values:
        return
    yes = sum(1 for v in values if v >= 0.5)
    print(f"    avg {statistics.mean(values):.2f}, true (>= 0.5) in {yes} of {len(values)} ({yes / len(values):.1%})")
    for low, high in NOUL_BUCKETS:
        count = sum(1 for v in values if low <= v < high)
        print(f"    {f'{low:.2f} - {min(high, 1):.2f}':<{LABEL_WIDTH}} {bar(count, len(values))}")


PRINTERS = {"choice": print_choice, "score": print_score, "noul": print_noul}

records = 0
skipped = 0
events_by_topic = Counter()
jev_ms = []
end_to_end_ms = []
attempts = Counter()
models = Counter()
input_tokens = []
output_tokens = []
answers_by_question = Counter()
answer_records = 0
# Every answer object, grouped by question.
answers = defaultdict(list)

consumer = Consumer(config)
try:
    selected_topics, summary_topics, topic_metadata = discover_topics(consumer)
    for topic in selected_topics:
        events_by_topic[topic] = 0

    for message in read_records(consumer, selected_topics, topic_metadata):
        events_by_topic[message.topic()] += 1
        if message.topic() not in summary_topics:
            continue
        try:
            record = json.loads(message.value())
            evaluation = record["evaluation"]
            jev = record["jev"]
        except (TypeError, ValueError, KeyError):
            skipped += 1
            continue

        records += 1
        jev_ms.append(evaluation["duration_ms"])
        attempts[evaluation.get("attempt_count", 1)] += 1
        models[jev.get("model", "unknown")] += 1
        try:
            delay = parse_time(evaluation["completed_at"]) - parse_time(record["source"]["timestamp"])
            end_to_end_ms.append(delay.total_seconds() * 1000)
        except (KeyError, ValueError):
            pass

        usage = jev.get("usage", {})
        input_tokens.append(usage.get("input_tokens", 0))
        output_tokens.append(usage.get("output_tokens", 0))
        if message.topic() == "messages_jev":
            answer_records += 1
            for question, answer in jev.get("answers", {}).items():
                answers_by_question[question] += 1
                answers[question].append(answer)
finally:
    consumer.close()

print("Events by topic group")
grouped_topics = set()
for group, configured_topics in TOPIC_GROUPS:
    topics = [topic for topic in selected_topics if topic in configured_topics]
    if not topics:
        continue
    grouped_topics.update(topics)
    print(f"  {group} ({sum(events_by_topic[topic] for topic in topics):,})")
    for topic in topics:
        print(f"    {topic:<38} {events_by_topic[topic]:>8,}")
other_topics = [topic for topic in selected_topics if topic not in grouped_topics]
if other_topics:
    print(f"  Other ({sum(events_by_topic[topic] for topic in other_topics):,})")
    for topic in other_topics:
        print(f"    {topic:<38} {events_by_topic[topic]:>8,}")
print(f"  {'Total':<40} {sum(events_by_topic.values()):>8,}")
print()

if not records:
    sys.exit("No Enriched Records found in the selected topics")

total_input = sum(input_tokens)
total_output = sum(output_tokens)

print(f"Enriched records summarized: {records}" + (f" ({skipped} unreadable events skipped)" if skipped else ""))
print()
print("Latency (ms)")
print_latency_table({"Jev call": jev_ms, "End to end": end_to_end_ms})
print()
print("  Attempts per record: " + ", ".join(f"{n}: {count}" for n, count in sorted(attempts.items())))
print()
print("Token usage")
print(f"  {'Input':<22} total {total_input:>10,}  avg {total_input / records:>8.0f}")
print(f"  {'Output':<22} total {total_output:>10,}  avg {total_output / records:>8.0f}")
print(f"  {'Total':<22} total {total_input + total_output:>10,}  avg {(total_input + total_output) / records:>8.0f}")
print()
# Only input tokens are billed.
cost = total_input / 1_000_000 * args.input_price
print(f"Estimated cost (USD ${args.input_price} per million input tokens, output free)")
print(f"  {'Total':<22} ${cost:,.4f}")
print(f"  {'Per record':<22} ${cost / records:,.8f}")
print(f"  {'Per 1,000 records':<22} ${cost / records * 1000:,.4f}")
print(f"  {'Per 1 million records':<22} ${cost / records * 1_000_000:,.2f}")
print()
print(f"Questions answered from messages_jev ({answer_records} records): {sum(answers_by_question.values())}")
for question, count in answers_by_question.most_common():
    print(f"  {question:<26} {count}")
print()
print("Answer distribution from messages_jev")
for question, items in answers.items():
    kind = items[0].get("type", "unknown")
    print(f"  {question} ({kind}, {len(items)} answers)")
    printer = PRINTERS.get(kind)
    if printer:
        printer(items)
    else:
        print("    unknown answer type")
print()
print("Models: " + ", ".join(f"{model} ({count})" for model, count in models.most_common()))
