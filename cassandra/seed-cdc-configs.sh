#!/usr/bin/env bash
#
# Seeds the Sidecar's CDC and Kafka settings into sidecar_internal.configs.
#
# The Sidecar reads those two rows rather than its own YAML, so that an operator can retune a
# running publisher; nothing creates them, so a stack with no rows publishes nothing and says
# only "config is not ready" in its log.  The Sidecar creates the table itself, on the node that
# holds the cluster lease, which is why this waits for the table rather than creating it: a
# CREATE TABLE here would race that one.
#
# IF NOT EXISTS on each insert makes a restart cheap and leaves a hand-edited row alone.
set -euo pipefail

CASSANDRA_HOST="${1:?usage: seed-cdc-configs.sh <cassandra-host>}"
KAFKA_BOOTSTRAP="${CDC_KAFKA_BOOTSTRAP:-kafka:19092}"
CDC_TOPIC="${CDC_TOPIC:-cdc-mutations}"
SCHEMA_REGISTRY_URL="${CDC_SCHEMA_REGISTRY_URL:-http://apicurio:8080/apis/ccompat/v7}"
TIMEOUT_S="${CDC_SEED_TIMEOUT_S:-300}"

deadline=$((SECONDS + TIMEOUT_S))
until cqlsh "${CASSANDRA_HOST}" -e "SELECT service FROM sidecar_internal.configs LIMIT 1;" >/dev/null 2>&1; do
  if [ "${SECONDS}" -ge "${deadline}" ]; then
    echo "CDC seed: sidecar_internal.configs did not appear within ${TIMEOUT_S}s; CDC will not publish."
    exit 1
  fi
  sleep 5
done

# watermark_seconds is the age at which the publisher stops waiting for a mutation to reach its
# consistency level and drops it.  1800 rather than the 4h default: at RF=1 a mutation is
# complete when it is written, and a shorter window bounds the state this node keeps.
#
# micro_batch_delay_millis is the pause between reads of cdc_raw, so it is also the floor of the
# demo's end-to-end latency.  500 ms reads twice a second, which the UI can show.
#
# max_commit_logs is 2 rather than 4, because each open log is a whole 32MiB segment held in the
# Sidecar's heap by BufferingCommitLogReader, and that heap is 2 GB: the Sidecar starts with no
# -Xmx, so it takes 25% of the cassandra container's limit.  Four logs exhausted it, the reader
# then retried the same segment forever, and the garbage-collection thrash stalled every other
# Sidecar thread, which is how a CDC setting failed the spark_bulk access path in CI.
# ../entrypoint.sh records the whole chain, and why the segment size is not the lever.
#
# Two is the floor as well as the setting: at 1 the reader publishes nothing at all.  It polls
# GET /api/v1/cdc/segments about ten times a second, re-registers the schema each time and never
# reads a segment; measured over 183 s, 3,758 writes a second reached the table and 0 records
# reached the topic.  So there is no lower value to fall back to, and the OOM above has to be paid
# for out of heap rather than out of concurrency.
#
# What two costs is headroom, not throughput.  The publisher's ceiling is the same order as the
# demo's own write rate to the table, so which way the queue moves depends on what the sink is
# doing: measured across four windows on one stack, 4,144 and 4,349 records/s published against
# about 2,010 writes/s while it drained a backlog it had built, then 2,410 published against 3,148
# written while the sink drained its own Kafka backlog after a restart.  A ratio above 1 is the
# publisher catching up and below 1 is it falling behind; neither is duplication, which was
# checked and is absent.  The consequence is that the latency figure the demo quotes holds only
# while the writer is the slower of the two.  A fifth window put the margin at 7%, 2,882 records/s
# against 2,703 written, and the age held at 836 to 848 s throughout: once a backlog exists, a ratio
# just above 1 needs hours to close it.  Raising the Sidecar's heap and returning to four
# logs is the untried lever, and it wants headroom the container's 8 GB does not obviously have,
# since Cassandra holds 4 GB of it.
#
# Each INSERT below is IF NOT EXISTS, so changing a value here reaches only a stack whose
# sidecar_internal.configs is empty.  An existing stack needs a wipe or a hand-written UPDATE.
cqlsh "${CASSANDRA_HOST}" <<CQL
INSERT INTO sidecar_internal.configs (service, config)
VALUES ('cdc', {
  'cdc_enabled':                 'true',
  'topic':                       '${CDC_TOPIC}',
  'topic_format_type':           'STATIC',
  'jobid':                       'htap-demo',
  'datacenter':                  'datacenter1',
  'watermark_seconds':           '1800',
  'micro_batch_delay_millis':    '500',
  'max_commit_logs':             '2',
  'persist_state':               'true',
  'fail_kafka_errors':           'true',
  'fail_kafka_too_large_errors': 'false'
}) IF NOT EXISTS;
CQL

# The value serializer is Confluent's KafkaAvroSerializer, which registers one Avro schema per
# topic and writes its id into every record; schema.registry.url points at Apicurio's
# Confluent-compatible endpoint.  Apicurio is here rather than cp-schema-registry because this
# stack claims Apache-licensed components throughout and the Confluent build is not one.
cqlsh "${CASSANDRA_HOST}" <<CQL
INSERT INTO sidecar_internal.configs (service, config)
VALUES ('kafka', {
  'bootstrap.servers':   '${KAFKA_BOOTSTRAP}',
  'key.serializer':      'org.apache.kafka.common.serialization.StringSerializer',
  'value.serializer':    'io.confluent.kafka.serializers.KafkaAvroSerializer',
  'schema.registry.url': '${SCHEMA_REGISTRY_URL}',
  'acks':                'all',
  'retries':             '3',
  'linger.ms':           '5',
  'batch.size':          '16384'
}) IF NOT EXISTS;
CQL

echo "CDC seed: configs seeded, topic ${CDC_TOPIC}, registry ${SCHEMA_REGISTRY_URL}."
