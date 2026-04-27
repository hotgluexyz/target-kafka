"""Confluent Kafka producer wrapper used by the target."""

from __future__ import annotations

import json
import logging
import struct
from threading import Lock
from typing import Any, Callable, Dict, Optional, Set, Tuple

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


logger = logging.getLogger(__name__)


SCHEMA_REGISTRY_MAGIC_BYTE = b"\x00"


class JsonSchemaRegistryValueSerializer:
    """Serialize a JSON-encodable record to the Confluent Schema Registry wire format.

    Wire format: ``<magic byte 0x00><big-endian uint32 schema id><JSON body>``.

    The schema id is looked up (or auto-registered) once per serializer instance and
    then cached; subsequent calls just encode the body.
    """

    def __init__(
        self,
        sr_client: Any,
        subject: str,
        schema_str: str,
        auto_register: bool = True,
        to_dict: Optional[Callable[[Any, Any], Any]] = None,
    ) -> None:
        from confluent_kafka.schema_registry import Schema

        self._sr_client = sr_client
        self._subject = subject
        self._schema = Schema(schema_str, schema_type="JSON")
        self._auto_register = auto_register
        self._to_dict = to_dict
        self._schema_id: Optional[int] = None
        self._lock = Lock()

    def _resolve_schema_id(self) -> int:
        if self._schema_id is not None:
            return self._schema_id
        with self._lock:
            if self._schema_id is None:
                if self._auto_register:
                    schema_id = self._sr_client.register_schema(self._subject, self._schema)
                else:
                    registered = self._sr_client.get_latest_version(self._subject)
                    schema_id = registered.schema_id
                self._schema_id = int(schema_id)
                logger.info(
                    "Schema Registry: using schema id %s for subject %r",
                    self._schema_id,
                    self._subject,
                )
        return self._schema_id

    def __call__(self, obj: Any, ctx: Any = None) -> bytes:
        if self._to_dict is not None:
            obj = self._to_dict(obj, ctx)
        schema_id = self._resolve_schema_id()
        body = json.dumps(obj).encode("utf-8")
        return SCHEMA_REGISTRY_MAGIC_BYTE + struct.pack(">I", schema_id) + body


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


def build_schema_registry_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Build SchemaRegistryClient config when `schema_registry_enabled` is true.

    Reads the flattened target config:
      - ``schema_registry_url`` (required)
      - ``schema_registry_api_key`` / ``schema_registry_api_secret`` (optional;
        both must be provided together for HTTP basic auth).
    """
    url = config.get("schema_registry_url")
    if not url:
        raise ValueError(
            "`schema_registry_enabled` is true but `schema_registry_url` is not set."
        )
    conf: Dict[str, Any] = {"url": url}

    api_key = config.get("schema_registry_api_key")
    api_secret = config.get("schema_registry_api_secret")
    
    if bool(api_key) != bool(api_secret):
        raise ValueError(
            "`schema_registry_api_key` and `schema_registry_api_secret` must be "
            "provided together."
        )
    if api_key and api_secret:
        conf["basic.auth.user.info"] = f"{api_key}:{api_secret}"
    return conf


class KafkaProducerClient:
    """Lazy, thread-safe Kafka producer wrapper shared across sinks."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self._config = config
        self._producer: Optional[Producer] = None
        self._admin_client: Optional[AdminClient] = None
        self._sr_client = None
        self._serializer_cache: Dict[Tuple[str, str, str], Callable] = {}
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

    @property
    def schema_registry_enabled(self) -> bool:
        return bool(self._config.get("schema_registry_enabled"))

    @property
    def schema_registry_client(self):
        """Lazy SchemaRegistryClient. Returns None when SR is not enabled."""
        if not self.schema_registry_enabled:
            return None
        if self._sr_client is None:
            with self._lock:
                if self._sr_client is None:
                    from confluent_kafka.schema_registry import SchemaRegistryClient

                    sr_conf = build_schema_registry_config(self._config)
                    safe_conf = {
                        k: ("***" if "auth" in k or "password" in k or "secret" in k else v)
                        for k, v in sr_conf.items()
                    }
                    logger.info("Initializing Schema Registry client with config: %s", safe_conf)
                    self._sr_client = SchemaRegistryClient(sr_conf)
        return self._sr_client

    def get_value_serializer(
        self,
        topic: str,
        schema: Dict[str, Any],
        to_dict: Optional[Callable[[Any, Any], Any]] = None,
    ) -> Optional[Callable]:
        """Build (and cache) a JSON Schema Registry serializer for ``topic``.

        Returns ``None`` when SR is not enabled. ``schema`` is the Singer stream's
        JSON Schema dict; it is registered automatically on first use.
        """
        sr_client = self.schema_registry_client
        if sr_client is None:
            return None

        if not isinstance(schema, dict) or not schema:
            raise ValueError(
                f"No JSON Schema available for topic {topic!r}; cannot serialize "
                "via Schema Registry."
            )

        schema_str = json.dumps(schema)
        cache_key = (topic, "json", schema_str)
        cached = self._serializer_cache.get(cache_key)
        if cached is not None:
            return cached

        serializer = JsonSchemaRegistryValueSerializer(
            sr_client=sr_client,
            subject=f"{topic}-value",
            schema_str=schema_str,
            auto_register=True,
            to_dict=to_dict,
        )
        self._serializer_cache[cache_key] = serializer
        return serializer

    def create_topic_if_not_exists(
        self, topic: str, num_partitions: int = 1, replication_factor: int = 1, timeout: float = 10
    ) -> None:
        if topic in self._topic_checked:
            return
        topic_metadata = self.admin_client.list_topics(timeout=timeout).topics.get(topic)
        if topic_metadata is None:
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
