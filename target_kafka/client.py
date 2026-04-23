"""Confluent Kafka producer wrapper used by the target."""

from __future__ import annotations

import logging
from threading import Lock
from typing import Any, Dict, Optional, Set

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


logger = logging.getLogger(__name__)


def build_connection_config(config: Dict[str, Any]) -> Dict[str, Any]:
    connection_conf: Dict[str, Any] = {"bootstrap.servers": config["bootstrap_servers"]}
    security_protocol = config.get("security_protocol")
    sasl_username = config.get("sasl_username")
    sasl_password = config.get("sasl_password")
    if sasl_username and sasl_password and not security_protocol:
        security_protocol = "SASL_SSL"
    if security_protocol:
        connection_conf["security.protocol"] = security_protocol
    if security_protocol and str(security_protocol).startswith("SASL"):
        connection_conf["sasl.mechanisms"] = config.get("sasl_mechanism", "PLAIN")
        if sasl_username:
            connection_conf["sasl.username"] = sasl_username
        if sasl_password:
            connection_conf["sasl.password"] = sasl_password
    return connection_conf


def build_producer_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Translate target config into librdkafka producer config.

    Only the SASL settings are defaulted for Confluent Cloud; anything passed
    under `extra_producer_config` is merged last and wins, so power users can
    override any librdkafka setting (e.g. `compression.type`, `linger.ms`,
    `acks`, TLS overrides, etc.).
    """
    producer_conf = build_connection_config(config)
    producer_conf.update(
        {
            "client.id": config.get("client_id", "target-kafka"),
            "acks": "all",
            "enable.idempotence": True,
        }
    )

    extra = config.get("extra_producer_config") or {}
    if not isinstance(extra, dict):
        raise ValueError("`extra_producer_config` must be an object/dict of librdkafka settings.")
    producer_conf.update(extra)

    return producer_conf


class KafkaProducerClient:
    """Lazy, thread-safe Kafka producer wrapper shared across sinks."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self._config = config
        self._producer: Optional[Producer] = None
        self._admin_client: Optional[AdminClient] = None
        self._lock = Lock()
        self._delivery_error: Optional[str] = None
        self._topic_checked: Set[str] = set()

    @property
    def producer(self) -> Producer:
        if self._producer is None:
            with self._lock:
                if self._producer is None:
                    producer_conf = build_producer_config(self._config)
                    safe_conf = {
                        k: ("***" if "password" in k or "secret" in k else v)
                        for k, v in producer_conf.items()
                    }
                    logger.info("Initializing Kafka producer with config: %s", safe_conf)
                    self._producer = Producer(producer_conf)
        return self._producer

    @property
    def admin_client(self) -> AdminClient:
        if self._admin_client is None:
            with self._lock:
                if self._admin_client is None:
                    self._admin_client = AdminClient(build_connection_config(self._config))
        return self._admin_client

    def create_topic_if_not_exists(
        self, topic: str, num_partitions: int = 1, replication_factor: int = 1, timeout: float = 10
    ) -> None:
        if topic in self._topic_checked:
            return
        topic_metadata = self.admin_client.list_topics(timeout=timeout).topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            logger.info(f"Creating topic {topic}")
            future = self.admin_client.create_topics(
                [NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)]
            )[topic]
            future.result(timeout=timeout)
        self._topic_checked.add(topic)

    def produce(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
        headers: Optional[Dict[str, bytes]] = None,
    ) -> None:
        """Enqueue a message. Backpressures via `poll(1)` if local queue is full."""
        self._raise_if_delivery_failed()
        producer = self.producer
        while True:
            try:
                producer.produce(
                    topic=topic,
                    value=value,
                    key=key,
                    headers=headers,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                # Internal queue is full; drain callbacks and retry.
                producer.poll(1)
        producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        if self._producer is None:
            return 0
        remaining = self._producer.flush(timeout)
        self._raise_if_delivery_failed()
        if remaining > 0:
            raise RuntimeError(
                f"Kafka producer flush timed out with {remaining} messages still pending."
            )
        return remaining

    def _raise_if_delivery_failed(self) -> None:
        if self._delivery_error:
            err = self._delivery_error
            self._delivery_error = None
            raise RuntimeError(f"Kafka delivery failed: {err}")

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            topic = msg.topic() if msg is not None else "?"
            logger.error("Kafka delivery failed for topic=%s: %s", topic, err)
            # Store the first failure so the next produce/flush surfaces it.
            if self._delivery_error is None:
                self._delivery_error = str(err)
