package com.thelastpickle.htap.backend.read;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.thelastpickle.htap.backend.engine.CassandraPath;
import com.thelastpickle.htap.common.BucketKeys;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jboss.logging.Logger;

/**
 * Every read the dashboard makes over the CQL request path.
 *
 * <p>Each one is a point read or a bounded scan of a table holding one row per asset, so the
 * dashboard stays honest about what Cassandra is good at. A question CQL cannot express
 * belongs to one of the other four paths, and the compare page is where it is asked.
 */
@ApplicationScoped
public class CassandraReads {

    private static final Logger LOG = Logger.getLogger(CassandraReads.class);

    /**
     * One row per asset, so a full scan of {@code drone_latest_status} is bounded by fleet
     * size. Keep it in step with the producer's own maximum.
     */
    public static final int FLEET_SCAN_LIMIT = 5000;

    /**
     * Rows read from an asset's history to build a flight path. Sized so the path covers a
     * useful stretch of time at demo ingest rates while staying one bounded single-partition
     * read.
     */
    static final int TRAIL_SCAN_ROWS = 2000;

    private static final String LATEST_STATUS_COLUMNS =
            "entity_id, event_time, latitude, longitude, altitude_m, speed_mps, "
                    + "heading_deg, is_flying, temp_internal_c, temp_external_c, event_type, "
                    + "observer_id, telemetry_age_s, near_restricted_zone, predicted_zone_breach, "
                    + "risk_score";

    private final CassandraPath cassandra;
    private final IngestionRate ingestionRate;

    CassandraReads(CassandraPath cassandra, IngestionRate ingestionRate) {
        this.cassandra = cassandra;
        this.ingestionRate = ingestionRate;
    }

    // ────────── Overview / key performance indicator (KPI) queries ──────────

    /** Every fleet KPI, from one scan and one read of the ingestion counter. */
    public Kpis kpis() {
        List<FleetSample> samples = new ArrayList<>();
        // Six columns, where the map's read takes sixteen: these are polled every few
        // seconds over the whole fleet, and a column nothing shows is still coordinator
        // work and bytes on the wire. entity_id is here and unused for the reason
        // FleetSample gives.
        for (Row row : rows("SELECT entity_id, is_flying, speed_mps, altitude_m, "
                + "near_restricted_zone, predicted_zone_breach "
                + "FROM drone_latest_status LIMIT " + FLEET_SCAN_LIMIT)) {
            samples.add(new FleetSample(
                    row.getBoolean("is_flying"),
                    f64(row, "speed_mps"),
                    f32(row, "altitude_m"),
                    row.getBoolean("near_restricted_zone"),
                    row.getBoolean("predicted_zone_breach")));
        }
        long totalEvents = totalEvents();
        return Kpis.of(samples, totalEvents, ingestionRate.observe(totalEvents));
    }

    /** Fleet size. A count aggregate, so the coordinator returns one row. */
    public long droneCount() {
        for (Row row : rows("SELECT count(*) AS cnt FROM drone_latest_status")) {
            return row.getLong("cnt");
        }
        return 0L;
    }

    /** Every ingestion bucket summed. 0 before the sink has written any. */
    public long totalEvents() {
        try {
            long total = 0L;
            for (Row row : rows("SELECT record_count FROM ingestion_counts")) {
                total += row.isNull("record_count") ? 0L : row.getLong("record_count");
            }
            return total;
        } catch (RuntimeException e) {
            LOG.debugf("total events unavailable: %s", e);
            return 0L;
        }
    }

    // ──────────────────────── Map / fleet queries ────────────────────────

    /** Latest state per asset, for the map and the polygon tools. */
    public List<FleetRow> drones(int limit, boolean flyingOnly) {
        String where = flyingOnly ? "WHERE is_flying = true " : "";
        String suffix = flyingOnly ? " ALLOW FILTERING" : "";
        return fleetRows("SELECT " + LATEST_STATUS_COLUMNS + " FROM drone_latest_status "
                + where + "LIMIT " + Math.clamp(limit, 1, FLEET_SCAN_LIMIT) + suffix);
    }

    /** Single-partition point read: the query Cassandra is here for. */
    public Optional<FleetRow> drone(String entityId) {
        List<FleetRow> found = fleetRows(
                "SELECT " + LATEST_STATUS_COLUMNS + " FROM drone_latest_status WHERE entity_id = ?",
                entityId);
        return found.isEmpty() ? Optional.empty() : Optional.of(found.get(0));
    }

    /**
     * A thinned flight path for one asset, newest first.
     *
     * <p>A single-partition range scan of {@code drone_events_by_entity}, which is clustered
     * by {@code event_time DESC}: a sequential read, not a filtered scan.
     */
    public List<TrailRow> trail(String entityId, int points) {
        List<TrailRow> read = new ArrayList<>();
        for (Row row : rows("SELECT event_time, latitude, longitude, altitude_m, speed_mps, "
                        + "heading_deg FROM drone_events_by_entity WHERE entity_id = ? LIMIT "
                        + TRAIL_SCAN_ROWS,
                entityId)) {
            read.add(new TrailRow(
                    row.getInstant("event_time"),
                    f64(row, "latitude"),
                    f64(row, "longitude"),
                    f32(row, "altitude_m"),
                    f64(row, "speed_mps"),
                    f64(row, "heading_deg")));
        }
        return Trails.thin(read, Math.clamp(points, 2, 500));
    }

    /**
     * Every enabled restricted zone.
     *
     * <p>{@code enabled} is filtered here rather than in CQL so the table needs no index; it
     * holds a handful of rows of reference data seeded by the sink.
     */
    public List<ZoneRow> zones() {
        List<ZoneRow> zones = new ArrayList<>();
        for (Row row : rows("SELECT zone_id, zone_name, polygon_wkt, severity, enabled "
                + "FROM restricted_zones")) {
            // A null column counts as enabled, which is the default the Python asked for
            // and never got: its dict held every selected column, so `.get("enabled", True)`
            // read None rather than the default and dropped the zone. No seeded row has a
            // null here, so the two agree on this stack's data.
            boolean enabled = row.isNull("enabled") || row.getBoolean("enabled");
            if (enabled) {
                zones.add(new ZoneRow(
                        row.getString("zone_id"),
                        row.getString("zone_name"),
                        row.getString("polygon_wkt"),
                        row.getString("severity"),
                        true));
            }
        }
        return zones;
    }

    // ──────────────────────── Alerts ────────────────────────

    /**
     * Recent alerts, newest bucket first.
     *
     * <p>{@code alerts_by_bucket} is partitioned by hour, so this walks back one partition at
     * a time until it has enough rows.
     */
    public List<AlertRow> alerts(int limit, int hours) {
        List<AlertRow> alerts = new ArrayList<>();
        Instant bucketTime = Instant.now();
        for (int i = 0; i < Math.max(1, hours); i++) {
            int remaining = limit - alerts.size();
            if (remaining <= 0) {
                break;
            }
            for (Row row : rows("SELECT alert_id, alert_time, entity_id, alert_type, severity, "
                            + "zone_id, latitude, longitude, altitude_m, message, risk_score "
                            + "FROM alerts_by_bucket WHERE bucket = ? LIMIT " + remaining,
                    BucketKeys.hour(bucketTime))) {
                alerts.add(new AlertRow(
                        row.get("alert_id", UUID.class),
                        row.getInstant("alert_time"),
                        row.getString("entity_id"),
                        row.getString("alert_type"),
                        row.getString("severity"),
                        row.getString("zone_id"),
                        f64(row, "latitude"),
                        f64(row, "longitude"),
                        f32(row, "altitude_m"),
                        row.getString("message"),
                        f64(row, "risk_score")));
            }
            bucketTime = bucketTime.minus(Duration.ofHours(1));
        }
        return alerts.size() <= limit ? alerts : alerts.subList(0, limit);
    }

    // ──────────────────────── Ingestion volume ────────────────────────

    /** Ingestion counts in 30-minute buckets, oldest first. */
    public List<BucketCount> ingestionHistory(int hours) {
        List<BucketCount> buckets = new ArrayList<>();
        for (String key : historyBuckets(Instant.now(), hours)) {
            buckets.add(new BucketCount(key, bucketCount(key)));
        }
        return buckets;
    }

    /** The 30-minute keys covering the last {@code hours}, oldest first. */
    static List<String> historyBuckets(Instant now, int hours) {
        List<String> keys = new ArrayList<>(hours * 2);
        for (int i = hours * 2 - 1; i >= 0; i--) {
            keys.add(BucketKeys.thirtyMinute(now.minus(Duration.ofMinutes(30L * i))));
        }
        return keys;
    }

    private long bucketCount(String bucket) {
        try {
            for (Row row : rows("SELECT record_count FROM ingestion_counts WHERE bucket = ?",
                    bucket)) {
                return row.isNull("record_count") ? 0L : row.getLong("record_count");
            }
            return 0L;
        } catch (RuntimeException e) {
            // One unreadable bucket reads as a gap in the chart rather than losing the
            // series, which is what the Python did and what the chart is for.
            LOG.debugf("bucket %s unavailable: %s", bucket, e);
            return 0L;
        }
    }

    // ──────────────────────── Reading a row ────────────────────────

    private Iterable<Row> rows(String cql, Object... values) {
        // Through the path rather than the session, so a session that has stopped working is
        // marked disconnected and reconnected rather than failing every read from here on.
        return cassandra.execute(SimpleStatement.newInstance(cql, values));
    }

    private List<FleetRow> fleetRows(String cql, Object... values) {
        List<FleetRow> fleet = new ArrayList<>();
        for (Row row : rows(cql, values)) {
            fleet.add(new FleetRow(
                    row.getString("entity_id"),
                    row.getInstant("event_time"),
                    f64(row, "latitude"),
                    f64(row, "longitude"),
                    f32(row, "altitude_m"),
                    f64(row, "speed_mps"),
                    f64(row, "heading_deg"),
                    row.getBoolean("is_flying"),
                    f32(row, "temp_internal_c"),
                    f32(row, "temp_external_c"),
                    row.getBoolean("near_restricted_zone"),
                    row.getBoolean("predicted_zone_breach"),
                    f64(row, "risk_score")));
        }
        return fleet;
    }

    private static Double f64(Row row, String column) {
        return row.get(column, Double.class);
    }

    /**
     * A CQL {@code float} column, widened.
     *
     * <p>Read as a {@code float} and then widened, which is what the Python driver did and
     * what keeps the figures the same: 32-bit 12.3 widens to 12.300000190734863, and rounding
     * that to a tenth is the dashboard's 12.3. Reading the column as a double directly would
     * fail the driver's own type check rather than converting.
     */
    private static Double f32(Row row, String column) {
        Float value = row.get(column, Float.class);
        return value == null ? null : Double.valueOf(value.doubleValue());
    }
}
