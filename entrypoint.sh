#!/bin/sh
# Starts a distributed Kafka Connect worker with the Jev plugin from /opt/kafka/plugins
# and registers one or more connector configurations.
set -eu

WORKER_CONFIG=/etc/kafka-jev/connect-distributed.properties
CONNECTOR_CONFIG=/etc/kafka-jev/connector.json
CA_FILE=/etc/kafka-jev/kafka-ca.crt
CONNECTOR_NAME=${CONNECTOR_NAME:-jev-techzone}
REST_URL=http://localhost:8083

# Trust the cluster's private CA on top of the JVM defaults, so both the brokers and
# HTTPS to TypeSafe are trusted. Every certificate in the PEM file is imported.
if [ -s "$CA_FILE" ]; then
  TRUSTSTORE=/tmp/kafka-jev-truststore.jks
  STOREPASS=changeit
  cp "$JAVA_HOME/lib/security/cacerts" "$TRUSTSTORE"
  chmod u+w "$TRUSTSTORE"
  CERT_DIR=$(mktemp -d)
  awk -v dir="$CERT_DIR" '/BEGIN CERTIFICATE/ { n++ } n { print > (dir "/ca-" n ".pem") }' "$CA_FILE"
  for cert in "$CERT_DIR"/ca-*.pem; do
    keytool -importcert -noprompt -alias "kafka-$(basename "$cert" .pem)" -file "$cert" \
      -keystore "$TRUSTSTORE" -storepass "$STOREPASS" >/dev/null
  done
  rm -rf "$CERT_DIR"
  export KAFKA_OPTS="${KAFKA_OPTS:-} -Djavax.net.ssl.trustStore=$TRUSTSTORE -Djavax.net.ssl.trustStorePassword=$STOREPASS"
  echo "Trusting CA certificates from $CA_FILE"
fi

# Optional topic creation, for PLAINTEXT clusters only.
if [ -n "${CREATE_TOPICS:-}" ]; then
  : "${BOOTSTRAP_SERVERS:?BOOTSTRAP_SERVERS is required when CREATE_TOPICS is set}"
  for topic in $(echo "$CREATE_TOPICS" | tr ',' ' '); do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVERS" \
      --create --if-not-exists --topic "$topic"
  done
fi

/opt/kafka/bin/connect-distributed.sh "$WORKER_CONFIG" &
WORKER_PID=$!
trap 'kill -TERM "$WORKER_PID" 2>/dev/null' TERM INT

echo "Waiting for the Connect REST API..."
until curl --fail --silent "$REST_URL/connector-plugins" >/dev/null; do
  if ! kill -0 "$WORKER_PID" 2>/dev/null; then
    echo "Connect worker exited before its REST API came up" >&2
    exit 1
  fi
  sleep 2
done

register_connector() {
  name=$1
  config=$2
  if [ ! -f "$config" ]; then
    echo "Connector config does not exist: $config" >&2
    exit 1
  fi

  # PUT creates the connector or updates it, so config edits apply on restart.
  echo "Registering connector $name from $config"
  until curl --fail --silent --show-error -X PUT -H 'Content-Type: application/json' \
      --data @"$config" "$REST_URL/connectors/$name/config"; do
    if ! kill -0 "$WORKER_PID" 2>/dev/null; then
      echo "Connect worker exited before connector $name was registered" >&2
      exit 1
    fi
    echo "Registration of $name failed; retrying in 5s" >&2
    sleep 5
  done
  echo
}

# CONNECTORS is a comma-separated list of name:path pairs. When absent, retain
# the original single-connector behavior.
if [ -n "${CONNECTORS:-}" ]; then
  for connector in $(echo "$CONNECTORS" | tr ',' ' '); do
    name=${connector%%:*}
    config=${connector#*:}
    if [ "$name" = "$connector" ] || [ -z "$name" ] || [ -z "$config" ]; then
      echo "Invalid CONNECTORS entry: $connector (expected name:path)" >&2
      exit 1
    fi
    register_connector "$name" "$config"
  done
else
  register_connector "$CONNECTOR_NAME" "$CONNECTOR_CONFIG"
fi

wait "$WORKER_PID"
