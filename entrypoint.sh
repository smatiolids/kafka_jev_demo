#!/bin/sh
# Starts a distributed Kafka Connect worker with the Jev plugin from /opt/kafka/plugins
# and registers the connector from /etc/kafka-jev/connector.json.
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

# PUT creates the connector or updates it, so edits to connector.json apply on restart.
echo "Registering connector $CONNECTOR_NAME"
until curl --fail --silent --show-error -X PUT -H 'Content-Type: application/json' \
    --data @"$CONNECTOR_CONFIG" "$REST_URL/connectors/$CONNECTOR_NAME/config"; do
  if ! kill -0 "$WORKER_PID" 2>/dev/null; then
    echo "Connect worker exited before the connector was registered" >&2
    exit 1
  fi
  echo "Registration failed; retrying in 5s" >&2
  sleep 5
done
echo

wait "$WORKER_PID"
