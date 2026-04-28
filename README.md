# target-kafka

`target-kafka` is a Singer target that publishes records to [Apache Kafka](https://kafka.apache.org/) / [Confluent Cloud](https://www.confluent.io/confluent-cloud/), built with the Meltano SDK for Singer Targets and the [`confluent-kafka`](https://github.com/confluentinc/confluent-kafka-python) Python client.

Each Singer `RECORD` is JSON-encoded and sent to a Kafka topic derived from the stream name. One `Producer` instance is shared across all streams, and the target flushes on every Singer `STATE` boundary so state is never acknowledged ahead of durable writes to Kafka.

## Installation

```bash
pipx install poetry
poetry install
```

## Configuration

| Setting                 | Required | Default                        | Description                                                                                           |
| ----------------------- | -------- | ------------------------------ | ----------------------------------------------------------------------------------------------------- |
| `bootstrap_servers`     | Yes      | —                              | Comma-separated `host:port` list. Confluent Cloud gives you this on the cluster settings page.        |
| `security_protocol`     | No       | `SASL_SSL` if SASL creds set, else `PLAINTEXT` | One of `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`.                             |
| `sasl_mechanism`        | No       | `PLAIN`                        | Used when `security_protocol` starts with `SASL`.                                                     |
| `sasl_username`         | No       | —                              | Confluent Cloud API key (or other SASL username).                                                     |
| `sasl_password`         | No       | —                              | Confluent Cloud API secret (or other SASL password).                                                  |
| `topic_prefix`          | No       | `""`                           | Prepended to the stream name when resolving a topic.                                                  |
| `stream_topic_map`      | No       | `{}`                           | `{ "<stream>": "<topic>" }` overrides. Takes precedence over `topic_prefix`.                          |
| `client_id`             | No       | `target-kafka`                 | Kafka `client.id`.                                                                                    |
| `flush_timeout`         | No       | `30`                           | Seconds to wait for in-flight messages at every drain / shutdown.                                     |
| `extra_producer_config` | No       | `{}`                           | Arbitrary librdkafka producer settings to merge in last (e.g. `{"compression.type": "lz4"}`).         |
| `schema_registry_url`   | No       | —                              | Schema Registry endpoint. When set, message values are serialized as JSON Schema via Confluent Schema Registry (see below). |
| `schema_registry_api_key` | No     | -                              | Schema Registry API key for HTTP basic auth. Must be paired with `schema_registry_api_secret`.         |
| `schema_registry_api_secret` | No  | -                              | Schema Registry API secret for HTTP basic auth. Must be paired with `schema_registry_api_key`.         |

Every record is produced with:

- **topic** = `stream_topic_map[stream]` if set, else `topic_prefix + stream`.
- **key** = `|`-joined values of the schema's `key_properties`, UTF-8 encoded (or `None` if no key properties).
- **value** = the record JSON-encoded as UTF-8. Datetimes, decimals and UUIDs are serialized as strings.

Sane defaults are applied on top of librdkafka: `acks=all` and `enable.idempotence=true` for at-least-once (actually exactly-once-per-producer-session) delivery.

### Schema Registry

Set `schema_registry_url` to publish message values in the
[Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
**JSON Schema** wire format (`<magic byte 0x00><big-endian uint32 schema id><JSON body>`).
SR serialization is enabled purely by the presence of this URL.
Each Singer stream's own schema is auto-registered on first use under the default
`TopicNameStrategy` subject (`<topic>-value`), and the resulting schema id is cached
for the lifetime of the target. Keys are still produced as plain UTF-8 strings.

> Records are encoded as UTF-8 JSON against the registered schema but are **not**
> client-side validated, so the target is compatible with toolchains that pin older
> versions of `jsonschema` (e.g. `pipelinewise-singer-python`). Downstream consumers
> that use Schema Registry serializers will still validate against the registered
> schema on read.

`schema_registry_api_key` and `schema_registry_api_secret` must be provided together;
when both are set the target uses them for HTTP basic auth against Schema Registry.
If neither is set, requests are made unauthenticated.

#### Example

```json
{
  "bootstrap_servers": "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092",
  "security_protocol": "SASL_SSL",
  "sasl_username": "YOUR_KAFKA_API_KEY",
  "sasl_password": "YOUR_KAFKA_API_SECRET",
  "schema_registry_url": "https://psrc-xxxxx.us-east-2.aws.confluent.cloud",
  "schema_registry_api_key": "YOUR_SR_API_KEY",
  "schema_registry_api_secret": "YOUR_SR_API_SECRET"
}
```

### Confluent Cloud example

```json
{
  "bootstrap_servers": "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092",
  "security_protocol": "SASL_SSL",
  "sasl_mechanism": "PLAIN",
  "sasl_username": "YOUR_CONFLUENT_API_KEY",
  "sasl_password": "YOUR_CONFLUENT_API_SECRET",
  "stream_topic_map": { "users": "prod.users.v1" }
}
```

### Local development (docker-compose)

A single-node Confluent broker is included for local testing:

```bash
docker compose up -d
```

Create the `users` topic (optional if doesn't exist already):

```bash
docker exec -it kafka-local \
  kafka-topics --bootstrap-server localhost:9092 \
               --create --topic users \
               --partitions 1 --replication-factor 1
```

Or, if you prefer an interactive shell inside the container:

```bash
docker exec -it kafka-local bash
kafka-topics --bootstrap-server localhost:9092 --create --topic users --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --list
kafka-topics --bootstrap-server localhost:9092 --describe --topic users
```

Then point the target at `localhost:9092` using [`.secrets/config.local.sample.json`](./.secrets/config.local.sample.json):

```json
{
  "bootstrap_servers": "localhost:9092",
  "security_protocol": "PLAINTEXT"
}
```

## Usage

```bash
poetry run target-kafka --version
poetry run target-kafka --help
```

Verify messages landed on the broker (local):

```bash
docker exec -it kafka-local \
  kafka-console-consumer --bootstrap-server localhost:9092 --topic users --from-beginning
```

## How streams map to topics

Given a Singer record with `stream = "users"` and no overrides, the target produces to topic `users`. With `topic_prefix = "prod."`, the topic becomes `prod.users`. With `stream_topic_map = {"users": "prod.users.v1"}`, the topic becomes `prod.users.v1` (overrides win over the prefix).

Topics are **not** auto-created by the target itself - it relies on broker-side auto-creation (`auto.create.topics.enable=true`, which the included `docker-compose.yml` enables, and which is the default on Confluent Cloud for basic clusters). Pre-create topics explicitly for production use.
