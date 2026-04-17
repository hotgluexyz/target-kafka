"""Kafka target sink classes."""

from __future__ import annotations

import datetime
import decimal
import json
import uuid
from typing import Any, Dict, List, Optional

from singer_sdk.plugin_base import PluginBase
from hotglue_singer_sdk.target_sdk.sinks import HotglueSink


class _JSONEncoder(json.JSONEncoder):
    """JSON encoder that handles datetimes, decimals and UUIDs."""

    def default(self, o: Any) -> Any:
        if isinstance(o, (datetime.datetime, datetime.date, datetime.time)):
            return o.isoformat()
        if isinstance(o, decimal.Decimal):
            return str(o)
        if isinstance(o, uuid.UUID):
            return str(o)
        if isinstance(o, bytes):
            return o.decode("utf-8", errors="replace")
        return super().default(o)


class KafkaSink(HotglueSink):
    """Generic Kafka sink that forwards any stream to a Kafka topic.

    The topic name is `stream_name` by default, optionally prefixed via
    `topic_prefix` or fully overridden via `stream_topic_map[stream_name]`.
    Message keys are derived from the schema's `key_properties` so that
    related records land on the same partition.
    """

    # Required by TargetHotglue._process_record_message even though we don't
    # use the Hotglue REST plumbing.
    allows_externalid: List[str] = []
    latest_state: Optional[Dict[str, Any]] = None

    # Flush per state-drain boundary instead of hoarding megabytes in memory.
    max_size = 1000

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict[str, Any],
        key_properties: Optional[List[str]],
    ) -> None:
        self._target = target
        super().__init__(target, stream_name, schema, key_properties)

    @property
    def name(self) -> str:
        return self.stream_name

    @property
    def topic(self) -> str:
        stream_topic_map = self.config.get("stream_topic_map") or {}
        if self.stream_name in stream_topic_map:
            return stream_topic_map[self.stream_name]
        prefix = self.config.get("topic_prefix") or ""
        return f"{prefix}{self.stream_name}"

    def preprocess_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        return record

    def process_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> None:
        """Produce a single record to Kafka. No local batching: Kafka's
        internal queue already handles batching/linger efficiently."""
        value = json.dumps(record, cls=_JSONEncoder).encode("utf-8")
        key = self._build_key(record)

        self._target.kafka_client.produce(
            topic=self.topic,
            value=value,
            key=key,
        )

    def process_batch(self, context: Dict[str, Any]) -> None:
        """Called at drain boundaries (state messages, shutdown). Flush so the
        target never acknowledges state before messages are on the broker."""
        timeout = float(self.config.get("flush_timeout", 30))
        self._target.kafka_client.flush(timeout=timeout)

    def _build_key(self, record: Dict[str, Any]) -> Optional[bytes]:
        if not self.key_properties:
            return None
        parts = [str(record.get(k, "")) for k in self.key_properties]
        return "|".join(parts).encode("utf-8")
