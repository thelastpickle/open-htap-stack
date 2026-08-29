package com.thelastpickle.htap.sink;

import java.util.ArrayList;
import java.util.List;

/**
 * The demo keyspace, and the only definition of it.
 *
 * <p>The sink owns the schema because it is the one process that has to exist for data to flow, so
 * there is no migration step to forget and no second copy to drift. Every statement is
 * {@code IF NOT EXISTS}, which is also the trap: an option is applied at {@code CREATE TABLE} and
 * never afterwards, so editing a key here and rebuilding changes nothing until the table is
 * dropped. See the {@code schema} skill.
 *
 * <p>Two options are conditional, and each is read from the same declaration the node reads.
 *
 * <p><b>{@code transactional_mode='full'} cannot be added later.</b> Measured on 6.0-alpha2: a
 * table must be born transactional, because {@code ALTER TABLE … WITH transactional_mode='full'}
 * is accepted but only starts a migration, after which the table reports
 * {@code transactional_migration_from='off'} and refuses every transaction with "Transaction
 * Statement is unsupported when … before migration to Accord is complete for a range". Finishing
 * that migration needs a repair, and at replication factor 1 {@code nodetool repair} declines with
 * "No repair is needed" while {@code nodetool consensus_admin finish-migration} fails in its own
 * JMX return path with {@code NotSerializableException: java.util.ArrayList$SubList}. So a data
 * directory whose session tables exist without the option needs
 * {@code ./stop-and-clean-data-and-schema.sh}. The option is also refused outright when the
 * subsystem is off, with "Cannot create table demo.x with transactional mode full with
 * accord.enabled set to false", which is why it is conditional at all: with Accord off an option
 * would stop the sink at its schema step.
 *
 * <p><b>{@code cdc} can be added later</b>, so it is written here and reconciled in
 * {@link SchemaOwner#ensureCdc}: the statement below covers a fresh keyspace, and the reconcile
 * covers a stack that predates the option.
 */
final class DemoSchema {

    /**
     * Dimensions of the text embedding the dashboard's search writes.
     *
     * <p>The backend's {@code vector.dimensions} must agree: a vector of another width is refused
     * by the column, and the two are the same declaration in different processes.
     */
    static final int EMBEDDING_DIMS = 1536;

    /**
     * Restricted airspace around Oslo, matching the producer's default fleet area.
     *
     * <p>Reference data, seeded {@code IF NOT EXISTS} and never truncated. The capacity is here
     * rather than in the clearance tables so that a zone's definition and its limit cannot
     * disagree, and the numbers are small on purpose: the transaction demo has to be able to
     * exhaust a zone within a handful of steps, where a capacity of 200 would put the interesting
     * refusal out of reach. A tighter limit on the two critical zones than on the warning one is
     * the only realism claimed.
     */
    static final List<DemoZone> ZONES = List.of(
            new DemoZone(
                    "zone-oslo-airport",
                    "Oslo Lufthavn Gardermoen",
                    "POLYGON((11.05 60.18, 11.15 60.18, 11.15 60.22, 11.05 60.22, 11.05 60.18))",
                    "critical",
                    2),
            new DemoZone(
                    "zone-royal-palace",
                    "Det Kongelige Slott",
                    "POLYGON((10.72 59.91, 10.74 59.91, 10.74 59.92, 10.72 59.92, 10.72 59.91))",
                    "critical",
                    3),
            new DemoZone(
                    "zone-fornebu",
                    "Fornebu Tech Park",
                    "POLYGON((10.62 59.88, 10.66 59.88, 10.66 59.90, 10.62 59.90, 10.62 59.88))",
                    "warning",
                    5));

    private DemoSchema() {}

    /**
     * Every statement the schema needs, in the order it must be run.
     *
     * <p>The keyspace first, and the index after the table it is on. Nothing else here depends on
     * order, but a list is what makes the whole schema one thing a test can read.
     */
    static List<String> statements(SinkSettings settings) {
        String keyspace = settings.keyspace();
        String transactional =
                settings.accordEnabled() ? " WITH transactional_mode='full'" : "";
        String cdc = settings.cdcEnabled() ? " WITH cdc = true" : "";

        List<String> statements = new ArrayList<>(16);
        statements.add("""
                CREATE KEYSPACE IF NOT EXISTS %s
                WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1 };"""
                .formatted(keyspace));

        // The raw event stream, one row per event: Presto and the Spark bulk reader read this
        // table and the tables below are projections of it.
        //
        // Partitioned by a time bucket rather than by event_id, so a question about a period of
        // time is a question about particular partitions. Keyed on the event alone, every path
        // had to read the whole table to answer "the last fifteen minutes", because event_time is
        // not part of the key and a token is a hash: there was nothing to prune on.
        //
        // The shard is what keeps those partitions a sane size. One bucket at the demo's default
        // rate is about 1.8M rows, far too much for one partition; over sixteen shards it is
        // nearer 110k. It comes from the event's own id rather than from the asset, so the spread
        // does not collapse when the fleet is small, and the cost is that a query for a whole
        // window names every shard.
        statements.add("""
                CREATE TABLE IF NOT EXISTS %s.%s (
                  event_bucket text,
                  shard int,
                  event_id timeuuid,
                  entity_id text,
                  event_day date,
                  event_time timestamp,
                  event_type text,
                  observer_id text,
                  latitude double,
                  longitude double,
                  altitude_m float,
                  temp_external_c float,
                  temp_internal_c float,
                  text_payload text,
                  PRIMARY KEY ((event_bucket, shard), event_id)
                );""".formatted(keyspace, settings.table()));

        // Latest state per asset: the live map and the fleet indicators. One row per asset, so a
        // full scan of it is bounded by fleet size.
        //
        // The one CDC table. It is the fleet's live state, so a mutation of it is the event a
        // downstream consumer would want, and at a hundred assets its rate is the demo's whole
        // write rate against a hundred keys. events deliberately stays out: opting it in would
        // put every raw row through the publisher, and the claim about the raw table is that the
        // analytical paths read it without a copy.
        statements.add("""
                CREATE TABLE IF NOT EXISTS %s.drone_latest_status (
                  entity_id text PRIMARY KEY,
                  event_id timeuuid,
                  event_time timestamp,
                  event_type text,
                  observer_id text,
                  latitude double,
                  longitude double,
                  altitude_m float,
                  temp_external_c float,
                  temp_internal_c float,
                  speed_mps double,
                  heading_deg double,
                  is_flying boolean,
                  telemetry_age_s int,
                  near_restricted_zone boolean,
                  predicted_zone_breach boolean,
                  risk_score double,
                  text_payload text,
                  updated_at timestamp
                )%s;""".formatted(keyspace, cdc));

        // The embeddings are in a table of their own for two reasons. PrestoDB's Cassandra
        // connector cannot parse the CQL vector type and drops the metadata of any table carrying
        // one, which would hide the live-status table from Presto entirely. And an embedding is
        // 1536 floats: keeping it out of the table the map reads on every refresh keeps that row
        // small. text_payload is stored beside the vector so a search result can show the snippet
        // the vector was built from, even after the producer has moved that asset on.
        statements.add("""
                CREATE TABLE IF NOT EXISTS %s.drone_text_embeddings (
                  entity_id text PRIMARY KEY,
                  text_payload text,
                  payload_vector vector<float, %d>,
                  updated_at timestamp
                );""".formatted(keyspace, EMBEDDING_DIMS));

        // No WITH OPTIONS, so the index takes the server's default similarity function. Stated
        // because it means a release that changes that default changes which neighbours this
        // index returns, silently rather than by failing. Naming the function here would fix the
        // demo to one metric, which is a decision worth taking deliberately.
        //
        // Murmur3 only, and this statement is where that becomes the whole stack's constraint:
        // every other partitioner is refused by name: "Storage-attached index does not support
        // the following IPartitioner implementations: [OrderPreservingPartitioner,
        // LocalPartitioner, ByteOrderedPartitioner, RandomPartitioner]", and being the schema
        // step, the refusal stops the sink and every table after it. Measured: on
        // ByteOrderedPartitioner the sink looped here forever having created three of the tables.
        // SAI is also the only vector index Cassandra has, so there is nothing weaker to fall
        // back to.
        statements.add("""
                CREATE CUSTOM INDEX IF NOT EXISTS payload_vector_idx
                ON %s.drone_text_embeddings (payload_vector)
                USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';""".formatted(keyspace));

        // Per-asset history: flight trails and per-asset analysis.
        statements.add("""
                CREATE TABLE IF NOT EXISTS %s.drone_events_by_entity (
                  entity_id text,
                  event_time timestamp,
                  event_id timeuuid,
                  event_type text,
                  observer_id text,
                  latitude double,
                  longitude double,
                  altitude_m float,
                  temp_external_c float,
                  temp_internal_c float,
                  speed_mps double,
                  heading_deg double,
                  zone_id text,
                  text_payload text,
                  PRIMARY KEY ((entity_id), event_time, event_id)
                ) WITH CLUSTERING ORDER BY (event_time DESC, event_id DESC);""".formatted(keyspace));

        statements.add("""
                CREATE TABLE IF NOT EXISTS %s.restricted_zones (
                  zone_id text PRIMARY KEY,
                  zone_name text,
                  polygon_wkt text,
                  severity text,
                  enabled boolean,
                  updated_at timestamp
                );""".formatted(keyspace));

        // Alerts, partitioned by hour so the dashboard reads whole partitions.
        statements.add("""
                CREATE TABLE IF NOT EXISTS %s.alerts_by_bucket (
                  bucket text,
                  alert_time timestamp,
                  entity_id text,
                  alert_id timeuuid,
                  alert_type text,
                  severity text,
                  zone_id text,
                  latitude double,
                  longitude double,
                  altitude_m float,
                  message text,
                  risk_score double,
                  PRIMARY KEY ((bucket), alert_time, entity_id, alert_id)
                ) WITH CLUSTERING ORDER BY (alert_time DESC, entity_id ASC, alert_id DESC);"""
                .formatted(keyspace));

        // Ingestion volume in 30-minute buckets, for the dashboard's throughput chart. The one
        // counter table, and the reason the Sidecar's CDC reader needs a workaround: a counter
        // column defeats its schema builder, which registers this table with the column as blob.
        statements.add("""
                CREATE TABLE IF NOT EXISTS %s.ingestion_counts (
                    bucket text PRIMARY KEY,
                    record_count counter
                );""".formatted(keyspace));

        // The three tables behind the session-timeline transaction: exactly-once and in-order.
        // sessions_open says the session exists, session_seq_applied records which sequence
        // numbers have been applied, and session_timeline is the projection. One transaction
        // reads all three and writes two, which is what no batch or lightweight transaction can
        // do: a batch is not conditional across partitions and a lightweight transaction
        // conditions on one partition, where these three have three different partition keys.
        statements.add("""
                CREATE TABLE IF NOT EXISTS %s.sessions_open (
                  user_id text,
                  session_id uuid,
                  PRIMARY KEY ((user_id), session_id)
                )%s;""".formatted(keyspace, transactional));
        statements.add("""
                CREATE TABLE IF NOT EXISTS %s.session_seq_applied (
                  user_id text,
                  session_id uuid,
                  seq bigint,
                  PRIMARY KEY ((user_id, session_id), seq)
                )%s;""".formatted(keyspace, transactional));
        statements.add("""
                CREATE TABLE IF NOT EXISTS %s.session_timeline (
                  user_id text,
                  session_id uuid,
                  seq bigint,
                  event_id timeuuid,
                  event_time timestamp,
                  event_type text,
                  payload text,
                  PRIMARY KEY ((user_id, session_id), seq)
                )%s;""".formatted(keyspace, transactional));

        // The reference table for the transaction measurement: the same columns and the same key
        // as session_timeline and deliberately not transactional. A figure for an Accord
        // transaction means nothing on its own, so the demo writes the same row three ways on the
        // same node in the same run: through the transaction, through a plain INSERT here, and
        // through an IF NOT EXISTS lightweight transaction here. The same table for the latter
        // two, so the comparison is not also a comparison of two table definitions.
        statements.add("""
                CREATE TABLE IF NOT EXISTS %s.session_timeline_plain (
                  user_id text,
                  session_id uuid,
                  seq bigint,
                  event_id timeuuid,
                  event_time timestamp,
                  event_type text,
                  payload text,
                  PRIMARY KEY ((user_id, session_id), seq)
                );""".formatted(keyspace));

        // The three tables behind the airspace-clearance transaction, which shows mutual
        // exclusion and a bounded resource: a drone may hold one clearance and a zone will clear
        // only so many at once. What makes it need Accord is that the two facts live in different
        // partitions, so a lightweight transaction can enforce "this drone holds nothing" or
        // "this zone has room" but never both, and two of them in sequence interleave.
        //
        // The counter is `remaining` rather than `granted`, and that is forced. Accord's CQL will
        // compare a LET reference to a literal but not to another LET reference: `IF occ.granted <
        // occ.capacity` is refused with "IllegalArgumentException null", where `IF occ.remaining >
        // 0` is accepted. Counting down against zero keeps the whole test inside the transaction;
        // counting up would mean the caller reading the capacity first and binding it, and a
        // concurrent change of capacity would not then be serialised against the grant.
        // `capacity` is kept so a reader can see the limit.
        statements.add("""
                CREATE TABLE IF NOT EXISTS %s.zone_occupancy (
                  zone_id text PRIMARY KEY,
                  zone_name text,
                  severity text,
                  capacity bigint,
                  remaining bigint
                )%s;""".formatted(keyspace, transactional));
        statements.add("""
                CREATE TABLE IF NOT EXISTS %s.zone_clearance (
                  zone_id text,
                  entity_id text,
                  granted_at timestamp,
                  PRIMARY KEY ((zone_id), entity_id)
                )%s;""".formatted(keyspace, transactional));
        statements.add("""
                CREATE TABLE IF NOT EXISTS %s.drone_clearance (
                  entity_id text PRIMARY KEY,
                  zone_id text,
                  granted_at timestamp
                )%s;""".formatted(keyspace, transactional));

        return List.copyOf(statements);
    }

    /**
     * One demo zone as it is seeded.
     *
     * @param capacity how many drones the zone will clear at once
     */
    record DemoZone(
            String zoneId, String zoneName, String polygonWkt, String severity, long capacity) {}
}
