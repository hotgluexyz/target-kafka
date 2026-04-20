"""Kafka target class."""

from __future__ import annotations

from pathlib import PurePath
from typing import List, Optional, Type, Union

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.target_sdk.target import TargetHotglue
from hotglue_singer_sdk.sinks import Sink


from target_kafka.client import KafkaProducerClient
from target_kafka.sinks import KafkaSink


class TargetKafka(TargetHotglue):
    """Singer target that writes records to Apache Kafka / Confluent Cloud."""

    name = "target-kafka"
    SINK_TYPES = [KafkaSink]

    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        state: str = None,
    ) -> None:
        self.config_file = config[0] if isinstance(config, list) else config
        super().__init__(config, parse_env_config, validate_config)
        self._kafka_client: Optional[KafkaProducerClient] = None

    config_jsonschema = th.PropertiesList(
        th.Property(
            "bootstrap_servers",
            th.StringType,
            required=True,
            description=(
                "Comma-separated list of Kafka bootstrap servers. "
                "For Confluent Cloud this looks like `pkc-xxxxx.us-east-1.aws.confluent.cloud:9092`."
            ),
        ),
        th.Property(
            "security_protocol",
            th.StringType,
            required=False,
            description=(
                "`PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL`. "
                "Defaults to `SASL_SSL` when `sasl_username`/`sasl_password` are provided, "
                "otherwise `PLAINTEXT`."
            ),
        ),
        th.Property(
            "sasl_mechanism",
            th.StringType,
            required=False,
            default="PLAIN",
            description="SASL mechanism to use when `security_protocol` starts with `SASL`.",
        ),
        th.Property(
            "sasl_username",
            th.StringType,
            required=False,
            description="Confluent Cloud API key (or other SASL username).",
        ),
        th.Property(
            "sasl_password",
            th.StringType,
            required=False,
            description="Confluent Cloud API secret (or other SASL password).",
        ),
        th.Property(
            "topic_prefix",
            th.StringType,
            required=False,
            default="",
            description="String to prepend to every stream name when resolving the Kafka topic.",
        ),
        th.Property(
            "stream_topic_map",
            th.ObjectType(),
            required=False,
            description=(
                "Explicit stream-name → topic-name overrides. "
                "Takes precedence over `topic_prefix`."
            ),
        ),
        th.Property(
            "client_id",
            th.StringType,
            required=False,
            default="target-kafka",
            description="Client id reported to the Kafka cluster.",
        ),
        th.Property(
            "flush_timeout",
            th.NumberType,
            required=False,
            default=30,
            description="Seconds to wait for in-flight messages on every state-drain / shutdown flush.",
        ),
        th.Property(
            "extra_producer_config",
            th.ObjectType(),
            required=False,
            description=(
                "Arbitrary librdkafka producer settings to merge in last (wins over defaults). "
                "Use dotted keys, e.g. `{\"compression.type\": \"lz4\", \"linger.ms\": 50}`."
            ),
        ),
    ).to_dict()

    @property
    def kafka_client(self) -> KafkaProducerClient:
        if self._kafka_client is None:
            self._kafka_client = KafkaProducerClient(dict(self.config))
        return self._kafka_client

    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        return KafkaSink


if __name__ == "__main__":
    TargetKafka.cli()
