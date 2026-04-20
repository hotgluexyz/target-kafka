"""Kafka target sink classes."""

from __future__ import annotations

import datetime
import decimal
import json
import uuid
from typing import Any, Dict, List, Optional

from hotglue_singer_sdk.target_sdk.client import HotglueBatchSink

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


class KafkaSink(HotglueBatchSink):
    """Generic Kafka sink that forwards any stream to a Kafka topic.

    The topic name is `stream_name` by default, optionally prefixed via
    `topic_prefix` or fully overridden via `stream_topic_map[stream_name]`.
    Message keys are derived from the schema's `key_properties` so that
    related records land on the same partition.
    """
    # Flush per state-drain boundary instead of hoarding megabytes in memory.
    max_size = 1000

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

    def make_batch_request(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        pass

    def process_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> None:
        """Produce a single record to Kafka and bookmark it in state.

        Kafka's internal queue already handles batching/linger, so we don't
        buffer records locally. Success is optimistic: any broker-side failure
        will surface when `process_batch` calls `flush()` at the drain
        boundary.
        """
        if not self.latest_state:
            self.init_state()

        value = json.dumps(record, cls=_JSONEncoder).encode("utf-8")
        record_id = self._record_id(record)

        self._target.kafka_client.produce(
            topic=self.topic,
            value=value,
            key=record_id.encode("utf-8") if record_id is not None else None,
        )

        state: Dict[str, Any] = {"success": True}
        if record_id is not None:
            state["id"] = record_id
        self.update_state(state, record=record)

    def process_batch(self, context: Dict[str, Any]) -> None:
        """Called at drain boundaries (state messages, shutdown). Flush so the
        target never acknowledges state before messages are on the broker."""
        timeout = float(self.config.get("flush_timeout", 30))
        self._target.kafka_client.flush(timeout=timeout)

    def _record_id(self, record: Dict[str, Any]) -> Optional[str]:
        if not self.key_properties:
            return None
        return "|".join(str(record.get(k, "")) for k in self.key_properties)
