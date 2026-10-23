# Kafka Jev Connector Demo

This demo runs Kafka Connect with the Jev connector against an existing Kafka cluster. The connector reads customer support tickets from the `messages` topic and sends each one to TypeSafe Jev, which classifies it: department, intent, urgency, dissatisfaction, cancellation risk and whether it needs a human. The result goes to `messages_jev`. Records the connector can't process go to `messages_jev_dlq`.

The connector is open source: [smatiolids/kafka-jev-connector](https://github.com/smatiolids/kafka-jev-connector). If you find this demo useful, please leave a ⭐ on the repository.

## What's in this folder

| File | Purpose |
| --- | --- |
| `plugin/` | The connector plugin zip, installed into the image (downloaded, not committed) |
| `config/connector.json` | Connector settings for an authenticated remote Kafka cluster |
| `config/connector-local.json` | Connector settings for the local plaintext Kafka broker |
| `config/questions.json` | The Jev questions, in readable form |
| `config/set_questions.py` | Copies `questions.json` into `connector.json` |
| `config/connect-distributed.properties` | Kafka Connect worker settings: cluster address and security |
| `config/connect-distributed-local.properties` | Plaintext Kafka Connect worker settings for local Kafka |
| `compose.kafka.yaml` | Standalone single-node Kafka for local development |
| `kafka-ca.crt` | CA certificate of the Kafka cluster |
| `.env` | Credentials (not committed) |
| `data/` | The ticket dataset (downloaded, not committed) |
| `helper/send_message.py` | Sends the dataset rows to Kafka |
| `helper/receive_messages.py` | Receives and labels records from the routed output topics |
| `helper/summarize_results.py` | Summarizes records across all `messages_jev*` topics |
| `helper/clear_messages.py` | Permanently clears records from all `messages*` topics |
| `helper/cancellation_risk.py` | Lists, as JSON, the tickets in `messages_jev` with cancellation risk |

## Prerequisites

- Docker with Docker Compose
- Python 3
- For a remote cluster, the topics `messages`, `messages_jev`, `messages_jev_urgent`, `messages_jev_normal` and `messages_jev_dlq` already exist
- Port 8083 is free

## Run Kafka and the connector locally

First [download the connector plugin](#2-download-the-connector-plugin). Then
create `.env` with the local Kafka addresses and your TypeSafe API key:

```bash
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
CONNECT_KAFKA_BOOTSTRAP_SERVERS=kafka:29092
CONNECT_WORKER_CONFIG_FILE=./config/connect-distributed-local.properties
CONNECTOR_CONFIG_FILE=./config/connector-local.json
CONNECTORS=jev:/etc/kafka-jev/config/connector-local.json,jev-router-by-choice:/etc/kafka-jev/config/connector-local-router-by-choice.json,jev-router-by-noul:/etc/kafka-jev/config/connector-local-router-by-noul.json,jev-router-by-score-level:/etc/kafka-jev/config/connector-local-router-by-score-level.json,jev-router-by-score-value:/etc/kafka-jev/config/connector-local-router-by-score-value.json

KAFKA_API_KEY=
KAFKA_API_SECRET=
KAFKA_SSL_CA_LOCATION=
TYPESAFE_API_KEY=<typesafe api key>

BOOTSTRAP_SERVERS=kafka:29092
CREATE_TOPICS=messages,messages_jev,messages_jev_dlq,messages_jev_billing,messages_jev_technical_support,messages_jev_account_security,messages_jev_retention,messages_jev_general_support,messages_jev_to_human,messages_jev_to_agent,messages_jev_dissatisfaction_0,messages_jev_dissatisfaction_1,messages_jev_dissatisfaction_2,messages_jev_dissatisfaction_3,messages_jev_urgent,messages_jev_normal
```

Start the local Kafka broker and the connector together:

```bash
docker compose -f compose.kafka.yaml -f compose.yaml up -d --build --wait
```

The host-side Python tools connect through `127.0.0.1:9092`; the Connect
container connects through Kafka's internal listener at `kafka:29092`. During
startup, the entrypoint creates all example topics and registers five connector
instances: fixed output, choice routing, noul routing, score-level routing, and
score-value routing. Each input record is evaluated by all five connectors, so
a batch of N records makes 5N Jev calls.

Check the running services and connector status:

```bash
docker compose -f compose.kafka.yaml -f compose.yaml ps
curl --fail --silent localhost:8083/connectors?expand=status
```

Follow the connector logs if startup or registration fails:

```bash
docker compose -f compose.kafka.yaml -f compose.yaml logs -f connect
```

Stop both services while preserving Kafka's local data:

```bash
docker compose -f compose.kafka.yaml -f compose.yaml down
```

Pass `--volumes` to the final command if you also want to delete the local
Kafka data.

## 1. Set credentials

Create `.env` in this folder:

```bash
KAFKA_BOOTSTRAP_SERVERS=163.66.85.138:9094,163.66.85.138:9095,163.66.85.138:9096
KAFKA_API_KEY=<kafka user>
KAFKA_API_SECRET=<kafka password>
KAFKA_SSL_CA_LOCATION=./kafka-ca.crt
TYPESAFE_API_KEY=<typesafe api key>
```

## 2. Download the connector plugin

Download the plugin zip from the [v0.2 release](https://github.com/smatiolids/kafka-jev-connector/releases/tag/v0.2) into `plugin/` and verify its checksum:

```bash
mkdir -p plugin && cd plugin
curl -fLO https://github.com/smatiolids/kafka-jev-connector/releases/download/v0.2/kafka-jev-connector-0.2-plugin.zip
curl -fLO https://github.com/smatiolids/kafka-jev-connector/releases/download/v0.2/kafka-jev-connector-0.2-plugin.zip.sha256
shasum -a 256 -c kafka-jev-connector-0.2-plugin.zip.sha256
cd ..
```

The check prints `kafka-jev-connector-0.2-plugin.zip: OK`. The image installs that version when it is built.

## 3. Download the dataset

The demo uses the [Multilingual Customer Support Tickets](https://www.kaggle.com/datasets/tobiasbueck/multilingual-customer-support-tickets/versions/8) dataset from Kaggle (version 8).

1. Open the link and sign in to Kaggle.
2. Click **Download** and choose **Download dataset as zip**.
3. Unzip it and copy `dataset-tickets-multi-lang3-4k.csv` into `data/`:

```bash
unzip ~/Downloads/archive.zip dataset-tickets-multi-lang3-4k.csv -d data/
```

The zip's name depends on your browser; adjust the path if needed. The file has 4,000 tickets in several languages. CSV files are not committed.

## 4. Start the connector

```bash
docker compose up -d --build --wait
curl -s localhost:8083/connectors/jev/status
```

The command returns once the connector is `RUNNING`.

## 5. Send tickets

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python helper/send_message.py --limit 10
```

The script sends each row of `data/dataset*.csv` to `messages` as a JSON object. Leave out `--limit` to send every row. Use `--skip N` to start after the first N rows, for example `--skip 10 --limit 10` sends rows 11 to 20. Add `--delay 1` to wait one second between messages. Each row is one call to the TypeSafe API, so start with a small `--limit`.

The classification appears in the `messages_jev` topic.

## 6. Receive output messages

Listen for new records produced to any topic whose name starts with
`messages_jev`. Newly created matching topics are discovered automatically,
and each record is preceded by a highlighted topic name:

```bash
.venv/bin/python helper/receive_messages.py --pretty
```

Use `--from-beginning` to include existing records, `--limit N` to stop after
N records, and `--timeout SECONDS` to stop after an idle period. Use
`--pattern REGEX` to select another topic pattern. Pass `--topic NAME` one or
more times to listen to exact topics instead, or set a comma-separated
`KAFKA_OUTPUT_TOPICS` value in `.env`.

## 7. Summarize the results

```bash
.venv/bin/python helper/summarize_results.py
```

The script discovers and reads every topic matching `^messages_jev.*` from the
beginning and prints:

- **Events by topic group:** input, fixed output, choice, noul, score level,
  score value, dead letter, and the overall total, with per-topic counts inside
  each group.
- **Latency:** the Jev call time (`evaluation.duration_ms`) and the end-to-end time from when the ticket was sent to when Jev finished, as average, p50, p95, p99 and max.
- **Token usage:** total and average input and output tokens.
- **Estimated cost:** at $0.042 per million input tokens (the Jev decision model price; output tokens are free). It shows the total, the cost per record, and the projected cost for 1,000 and 1 million records. Pass `--input-price` to use a different price.
- **Questions answered:** the total and per-question counts using only records
  from the fixed-output `messages_jev` topic.
- **Answer distribution** for each question, also using only `messages_jev`:
  - **choice:** how often each option was chosen, and the average confidence.
  - **score:** the average score and confidence, and how many scores fall on each level, rounded to the nearest one.
  - **noul:** the average value, how many are true (0.5 or higher), and the count in each quarter from 0 to 1.

## 8. List tickets with cancellation risk

```bash
.venv/bin/python helper/cancellation_risk.py > cancellation_risk.json
```

The script reads `messages_jev` from the beginning and prints a JSON object with every ticket whose `cancellation_risk` is 0.5 or higher, highest first. Each ticket has its key, offset, subject, language, the full ticket body and a compact copy of all its answers. Use `--threshold 0.7` to keep only the stronger cases, `--language pt` to keep one language, `--sort-by customer_dissatisfaction` to order by another question, `--no-text` to leave out the ticket body, and `--question` to filter on a different noul question, such as `requires_human`.

## 9. Clear message topics

Preview the matching topics without deleting anything:

```bash
.venv/bin/python helper/clear_messages.py
```

Permanently delete every record from all topics whose names start with
`messages` while preserving the topics and their configuration:

```bash
.venv/bin/python helper/clear_messages.py --yes
```

Use `--pattern REGEX` to select a different set of topics. Record deletion does
not reset Kafka's partition offset numbers.

## 10. Stop

```bash
docker compose down
```

## Changing the demo

- **Questions:** edit `config/questions.json`, run `python3 config/set_questions.py` to copy them into `config/connector.json`, then run `docker compose restart connect`.
- **Topics:** edit `config/connector.json`, then run `docker compose restart connect`.
- **Connector version:** download a different zip from the [releases page](https://github.com/smatiolids/kafka-jev-connector/releases) into `plugin/` (keep only one), then run `docker compose up -d --build --wait`.
- **Logs:** run `docker compose logs -f connect`.
