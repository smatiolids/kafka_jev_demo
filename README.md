# Kafka Jev Connector Demo

This demo runs Kafka Connect with the Jev connector against an existing Kafka cluster. The connector reads customer support tickets from the `messages` topic and sends each one to TypeSafe Jev, which classifies it: department, intent, urgency, dissatisfaction, cancellation risk and whether it needs a human. The result goes to `messages_jev`. Records the connector can't process go to `messages_jev_dlq`.

The connector is open source: [smatiolids/kafka-jev-connector](https://github.com/smatiolids/kafka-jev-connector). If you find this demo useful, please leave a ⭐ on the repository.

## What's in this folder

| File | Purpose |
| --- | --- |
| `plugin/` | The connector plugin zip, installed into the image (downloaded, not committed) |
| `config/connector.json` | Connector settings: topics, Jev questions and the template sent to Jev |
| `config/questions.json` | The Jev questions, in readable form |
| `config/set_questions.py` | Copies `questions.json` into `connector.json` |
| `config/connect-distributed.properties` | Kafka Connect worker settings: cluster address and security |
| `kafka-ca.crt` | CA certificate of the Kafka cluster |
| `.env` | Credentials (not committed) |
| `data/` | The ticket dataset (downloaded, not committed) |
| `send_message.py` | Sends the dataset rows to Kafka |
| `summarize_results.py` | Summarizes latency, token usage and answers in `messages_jev` |

## Prerequisites

- Docker with Docker Compose
- Python 3
- The topics `messages`, `messages_jev` and `messages_jev_dlq` already exist on the cluster
- Port 8083 is free

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

Download the plugin zip from the [v0.1 release](https://github.com/smatiolids/kafka-jev-connector/releases/tag/v0.1) into `plugin/` and verify its checksum:

```bash
mkdir -p plugin && cd plugin
curl -fLO https://github.com/smatiolids/kafka-jev-connector/releases/download/v0.1/kafka-jev-connector-0.1-plugin.zip
curl -fLO https://github.com/smatiolids/kafka-jev-connector/releases/download/v0.1/kafka-jev-connector-0.1-plugin.zip.sha256
shasum -a 256 -c kafka-jev-connector-0.1-plugin.zip.sha256
cd ..
```

The check prints `kafka-jev-connector-0.1-plugin.zip: OK`. Keep exactly one zip in `plugin/`; the image installs it when it is built.

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
curl -s localhost:8083/connectors/jev-confluent_platform/status
```

The command returns once the connector is `RUNNING`.

## 5. Send tickets

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python send_message.py --limit 10
```

The script sends each row of `data/dataset*.csv` to `messages` as a JSON object. Leave out `--limit` to send every row. Use `--skip N` to start after the first N rows, for example `--skip 10 --limit 10` sends rows 11 to 20. Add `--delay 1` to wait one second between messages. Each row is one call to the TypeSafe API, so start with a small `--limit`.

The classification appears in the `messages_jev` topic.

## 6. Summarize the results

```bash
.venv/bin/python summarize_results.py
```

The script reads `messages_jev` from the beginning and prints:

- **Latency:** the Jev call time (`evaluation.duration_ms`) and the end-to-end time from when the ticket was sent to when Jev finished, as average, p50, p95, p99 and max.
- **Token usage:** total and average input and output tokens.
- **Estimated cost:** at $0.042 per million input tokens (the Jev decision model price; output tokens are free). It shows the total, the cost per record, and the projected cost for 1,000 and 1 million records. Pass `--input-price` to use a different price.
- **Questions answered:** the total, and the count for each question.
- **Answer distribution** for each question:
  - **choice:** how often each option was chosen, and the average confidence.
  - **score:** the average score and confidence, and how many scores fall on each level, rounded to the nearest one.
  - **noul:** the average value, how many are true (0.5 or higher), and the count in each quarter from 0 to 1.

## 7. Stop

```bash
docker compose down
```

## Changing the demo

- **Questions:** edit `config/questions.json`, run `python3 config/set_questions.py` to copy them into `config/connector.json`, then run `docker compose restart connect`.
- **Topics:** edit `config/connector.json`, then run `docker compose restart connect`.
- **Connector version:** download a different zip from the [releases page](https://github.com/smatiolids/kafka-jev-connector/releases) into `plugin/` (keep only one), then run `docker compose up -d --build --wait`.
- **Logs:** run `docker compose logs -f connect`.
