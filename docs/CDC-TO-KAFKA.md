# Change Data Capture to Kafka

Cassandra's Change Data Capture (CDC), CEP-8, hard-links each commit log segment into `cdc_raw` and writes an index file beside it.&emsp;The Sidecar beside the node reads those segments, deserializes the mutations of a CDC-enabled table, and publishes each one to a Kafka topic as Avro.&emsp;Nothing queries Cassandra to do it, which is the property worth having: the change stream is taken from the write path's own log rather than by polling the request path.

It runs here.&emsp;`demo.drone_latest_status` opts in, the topic is `cdc-mutations`, and the **Streaming** page at <http://localhost:4000/streaming> shows the mutations arriving.

## The configuration, in three places

**The node.**&emsp;`cassandra/entrypoint.sh` patches four keys into `cassandra.yaml`.&emsp;A hard link cannot cross a filesystem, so `cdc_raw_directory` has to sit beside `commitlog_directory`, and both are under `/var/lib/cassandra`:

```yaml
cdc_enabled: true
cdc_raw_directory: /var/lib/cassandra/cdc_raw
cdc_total_space: 4096MiB
cdc_block_writes: false
```

**The table.**&emsp;`cdc` is a table option and, unlike `transactional_mode`, it can be turned on after the fact.&emsp;The sink owns the demo schema, so `ingress/consumer/consumer.py` declares it and reconciles it on every start:

```sql
ALTER TABLE demo.drone_latest_status WITH cdc = true;
```

**The publisher.**&emsp;The Sidecar reads its CDC and Kafka settings from `sidecar_internal.configs`, a table it creates itself, so that an operator can retune a running publisher.&emsp;Nothing creates those rows, and a stack with no rows publishes nothing and says only "config is not ready"; `cassandra/seed-cdc-configs.sh` writes them once the table appears:

```sql
INSERT INTO sidecar_internal.configs (service, config)
VALUES ('cdc', {
  'cdc_enabled': 'true',  'topic': 'cdc-mutations',  'topic_format_type': 'STATIC',
  'jobid': 'htap-demo',   'datacenter': 'datacenter1',
  'watermark_seconds': '1800',  'micro_batch_delay_millis': '500',
  'max_commit_logs': '4',       'persist_state': 'true'
}) IF NOT EXISTS;

INSERT INTO sidecar_internal.configs (service, config)
VALUES ('kafka', {
  'bootstrap.servers':   'kafka:19092',
  'value.serializer':    'io.confluent.kafka.serializers.KafkaAvroSerializer',
  'schema.registry.url': 'http://apicurio:8080/apis/ccompat/v7',
  'acks': 'all',  'retries': '3',  'linger.ms': '5',  'batch.size': '16384'
}) IF NOT EXISTS;
```

The registry is Apicurio 3.0.13, not `cp-schema-registry`: this stack claims Apache-licensed components throughout and the Confluent build is under the Confluent Community License.&emsp;Apicurio serves a Confluent-compatible API under `/apis/ccompat/v7`, which is the whole of what `KafkaAvroSerializer` and the dashboard both need.&emsp;Its storage is the image's in-process H2, so a restart loses the registered schema and the Sidecar registers it again on its next batch.

## What a record carries

One subject, `cdc-mutations-value`, holds one schema at id 1.&emsp;The record is an eleven-field envelope named `CassandraCDC`, and the row itself is a nested `payload` record whose fields are the table's nineteen columns.&emsp;Each column's Avro type carries the CQL type it was converted from as a `cqlType` property, which is what lets the page say `timestamp` where Avro says `long`.

```json
{
  "key": "demo:drone_latest_status:7a8f6e0f",
  "keyspace": "demo", "table": "drone_latest_status",
  "operation": "UPDATE", "partial": true,
  "mutation_at_ms": 1787744390524, "kafka_at_ms": 1787744396723, "age_ms": 6208.9,
  "columns": { "entity_id": "asset-000051", "speed_mps": 18.069453715605444, "…": "…" }
}
```

**The stream is of mutations, not of rows,** and two fields say so.&emsp;`operationType` reads `UPDATE` although the sink issued a CQL `INSERT`, because Cassandra has no distinction between them below the query layer.&emsp;`isPartial` is `true` because a mutation carries the cells it wrote and not the row as it now stands; `updateFields` names them.&emsp;A consumer that wants a row has to reassemble it, which is the ordinary cost of reading a log-structured store's log.

## Measured

A fresh stack, `podman compose up -d` at 11:25:03 UTC, first Kafka record at 11:26:38.653: **95.7 s to the first published mutation**.&emsp;The topic does not exist before that, because the Sidecar creates it with the first batch.

The figures below come from twenty-six one-minute samples over 1,447 s, each pairing the topic's end offsets with the dashboard's own counters.&emsp;The end offsets rose from 1,465,600 to 5,398,607, so **2,718 records/s** published; the rate follows the demo's write rate, and one-minute intervals ran from 1,905 to 3,779.&emsp;The tail read every record published in that span: `end offset − consumed` never exceeded 19,756, which is what the topic already held when the tail attached, so the lag did not grow.&emsp;**Zero decode failures** in 5.4 million records.

**End-to-end latency is seconds, and the commit log segment is why.**&emsp;`latency_p50_ms`, the age of a mutation when the dashboard decodes it, had a median of 8.0 s across those samples and was below 10 s in eighteen of the twenty-six; the range was 2.7 to 21.1 s.&emsp;The publisher's own `micro_batch_delay_millis` is 500 ms, so it is not the floor.&emsp;The floor is that a segment reaches the reader only once it is complete: segments here are 33,554,432 bytes each, and at this write rate ten consecutive ones were discarded 9.3 s apart, so a mutation waits half a segment on average.&emsp;A smaller `commitlog_segment_size` would lower that, and would cost more segments to track; the trade is not measured here.

| | |
| --- | --- |
| First record after `up -d` | 95.7 s |
| Publish rate | 2,718 records/s over 1,447 s |
| Consumer lag growth | none, over 3,933,007 records |
| Latency p50 | median 8.0 s, range 2.7 – 21.1 s |
| Segment period | 9.3 s, at 33,554,432 bytes each |
| `cdc_raw` growth | 3.6 MB/s, to a bound it then held for 13 minutes |
| Decode failures | 0 of 5,383,497 |

## What this costs, and what it does not guarantee

**A segment deleted before the Sidecar reads it is a gap in the stream, and no error says so.**&emsp;`cdc_total_space` bounds `cdc_raw`, and `cdc_block_writes` decides who enforces the bound.&emsp;Left at its default of `true`, Cassandra enforces it by **refusing the write**: measured here, the directory settled at 4,261,415,150 bytes in 127 segments and every write to `demo` was rejected for as long as it stood there.&emsp;This demo sets it `false`, so the node deletes the oldest segment instead.

Measured on this stack, twenty minutes into a run at 3.6 MB/s: `cdc_raw` reached a peak of 4,271,474,949 bytes in 130 segments and then held at 129 segments and about 4,261.5 MB for the following thirteen minutes, one segment short of the 4,294,967,296 limit.&emsp;The oldest segment advanced from `CommitLog-9-1787743506842` to `…866`, so twenty-four were deleted.&emsp;**Publication and the request path both continued** for as long as the directory stood at the bound: the topic's end offset rose from 3,218,781 to 5,398,607, and an `INSERT` into `demo.drone_latest_status` was accepted throughout.

**The loss is silent at the default log level.**&emsp;The node's message reads "Freed up {} ({}) bytes after deleting the oldest CDC commit log segments in non-blocking mode", and it is issued through `Logger.debug`, so nothing appears at INFO; established by disassembling `CommitLogSegmentManagerCDC$CDCSizeTracker` in this image's jar.&emsp;What is observable at INFO is the oldest segment's name advancing, which is what the measurement above rests on.

That trade is deliberate.&emsp;The claim this repository makes is that analytical and streaming machinery must not touch the OLTP request path, and a rejected `INSERT` is the loudest way to break it.&emsp;A deployment that would rather stall its writers than lose a change should set `cdc_block_writes: true` and give the publisher enough headroom that the bound is never reached.

**The node's deletion is the only trim there is.**&emsp;The Sidecar has its own `CdcRawDirectorySpaceCleaner`, and it cannot be made to fire first: it deletes only while the directory exceeds `cdc_total_space` times `cdc_raw_max_directory_max_percent`, whose default is 1.0, and the setting that would lower it cannot take a fractional value.&emsp;`CdcConfigurationImpl` declares the field a `float` but its setter takes a `long`, so `0.75` arrives as 0 and the cleaner empties the directory.&emsp;Measured here, and reported in `cassandra/sidecar.yaml`.

**One table, and `events` is deliberately out.**&emsp;CDC is per table, and putting the 2,000-writes-a-second event stream through it would fill `cdc_raw` far faster than the publisher drains it.&emsp;`drone_latest_status` is the interesting table in any case: it is the one a downstream consumer would follow.

**Replication-factor-aware deduplication is configured and not exercised.**&emsp;`watermark_seconds` is the age at which the publisher stops waiting for a mutation to be seen from enough replicas and publishes it anyway; it is 1800 here rather than the four-hour default, because at `replication_factor: 1` a mutation is complete as soon as it is written.&emsp;So the mechanism is in the path, and this one-node stack does not test it.

**Two local fixes were needed to make any of it run,** both in the Sidecar's own dependencies, and both because one commit log serves every table: the reader's schema must know every table in order to deserialize any segment.&emsp;A `counter` column and a `vector<float, 1536>` column each defeated that, and the second stopped the stream outright rather than costing one table's rows.&emsp;[`cassandra/dist/VENDOR.md`](../cassandra/dist/VENDOR.md) records both, with the bytecode each conclusion rests on.

## The Streaming page

`/streaming` is an ordinary Kafka consumer with a schema lookup, and it touches Cassandra not at all.&emsp;Two properties bound what it costs.&emsp;The tail keeps a fixed number of records, so a page left open overnight holds no more memory than one just opened; and it consumes whether or not anybody is watching, so what the page shows is what the topic did rather than what it did since somebody looked.

It reports the schema from the registry rather than from a record, because the point is that the contract lives there.&emsp;It also shows `isPartial` and `updateFields` on each row, so the mutation-not-row property above is visible rather than described.

The consumer has no group and commits no offsets: a restarted backend should show what is arriving now rather than replay from where the last one stopped.&emsp;It reads back one buffer's worth on attach so that a page opened after the fact has something to show, and flags those records, because their age measures the backlog rather than the pipeline.&emsp;`rate_per_sec` measures the consumer between two polls, not the publisher; read the publish rate from the topic's end offsets, as the table above does.
