FROM apache/kafka:4.2.0

USER root
RUN apk add --no-cache curl
COPY plugin/kafka-jev-connector-0.2-plugin.zip /tmp/plugin/kafka-jev-connector-plugin.zip
RUN mkdir -p /opt/kafka/plugins \
    && unzip -q /tmp/plugin/kafka-jev-connector-plugin.zip -d /tmp/plugin \
    && mv /tmp/plugin/kafka-jev-connector-*/ /opt/kafka/plugins/kafka-jev-connector \
    && chown -R appuser:appuser /opt/kafka/plugins \
    && rm -rf /tmp/plugin
COPY --chmod=755 entrypoint.sh /opt/kafka-jev/entrypoint.sh
USER appuser

ENTRYPOINT ["/opt/kafka-jev/entrypoint.sh"]
