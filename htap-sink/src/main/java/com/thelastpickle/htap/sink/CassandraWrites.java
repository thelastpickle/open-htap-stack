package com.thelastpickle.htap.sink;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.thelastpickle.htap.common.EventPartitions;
import com.thelastpickle.htap.sink.Alerts.Proximity;
import com.thelastpickle.htap.sink.DroneTracker.Derived;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * The five statements the sink writes, prepared once.
 *
 * <p>The three event writes are idempotent upserts, which is what makes a redelivered batch cost
 * duplicate work and no duplicate data. Nothing here is conditional, and nothing reads before
 * writing. Two are not idempotent and a replay does duplicate them: the counter update adds to
 * {@code record_count}, and an alert is written with an id minted per call, so a replay after a
 * restart -- where the cooldown map is gone -- writes a second row for the same proximity.
 *
 * <p>The three event writes carry QUORUM explicitly, where the driver's default is LOCAL_ONE. At
 * replication factor 1 the two are the same node and the same acknowledgement, so this buys nothing
 * today; it is what the demo would need the moment a second replica existed, and it costs a word.
 * The counter is left at the default, as it was in the Python.
 */
final class CassandraWrites implements Writes {

    private final CqlSession session;
    private final SinkSettings settings;
    private final PreparedStatement insertRaw;
    private final PreparedStatement insertHistory;
    private final PreparedStatement upsertLatest;
    private final PreparedStatement countIngested;
    private final PreparedStatement insertAlert;

    CassandraWrites(CqlSession session, SinkSettings settings) {
        this.session = session;
        this.settings = settings;
        String keyspace = settings.keyspace();
        this.insertRaw = session.prepare("INSERT INTO " + keyspace + "." + settings.table()
                + " (event_bucket, shard, event_id, entity_id, event_day, event_time, event_type,"
                + " observer_id, latitude, longitude, altitude_m, temp_external_c, temp_internal_c,"
                + " text_payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        this.insertHistory = session.prepare("INSERT INTO " + keyspace + ".drone_events_by_entity"
                + " (entity_id, event_time, event_id, event_type, observer_id, latitude, longitude,"
                + " altitude_m, temp_external_c, temp_internal_c, speed_mps, heading_deg, zone_id,"
                + " text_payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        this.upsertLatest = session.prepare("UPDATE " + keyspace + ".drone_latest_status SET"
                + " event_id = ?, event_time = ?, event_type = ?, observer_id = ?, latitude = ?,"
                + " longitude = ?, altitude_m = ?, temp_external_c = ?, temp_internal_c = ?,"
                + " speed_mps = ?, heading_deg = ?, is_flying = ?, telemetry_age_s = ?,"
                + " near_restricted_zone = ?, predicted_zone_breach = ?, risk_score = ?,"
                + " text_payload = ?, updated_at = ? WHERE entity_id = ?");
        this.countIngested = session.prepare("UPDATE " + keyspace + ".ingestion_counts"
                + " SET record_count = record_count + ? WHERE bucket = ?");
        this.insertAlert = session.prepare("INSERT INTO " + keyspace + ".alerts_by_bucket"
                + " (bucket, alert_time, entity_id, alert_id, alert_type, severity, zone_id,"
                + " latitude, longitude, altitude_m, message, risk_score)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    }

    @Override
    public List<CompletionStage<?>> event(
            Event event, Derived derived, Proximity proximity, Instant now) {
        int telemetryAge = telemetryAgeSeconds(event.eventTime(), now);
        return List.of(
                session.executeAsync(insertRaw.boundStatementBuilder()
                        .setConsistencyLevel(ConsistencyLevel.QUORUM)
                        .setString("event_bucket",
                                EventPartitions.bucket(event.eventTime(), settings.bucketMinutes()))
                        .setInt("shard", EventPartitions.shard(event.eventId(), settings.shards()))
                        .setUuid("event_id", event.eventId())
                        .setString("entity_id", event.entityId())
                        .setLocalDate("event_day", LocalDate.ofInstant(event.eventTime(), ZoneOffset.UTC))
                        .setInstant("event_time", event.eventTime())
                        .setString("event_type", event.eventType())
                        .setString("observer_id", event.observerId())
                        .setDouble("latitude", event.latitude())
                        .setDouble("longitude", event.longitude())
                        .setFloat("altitude_m", (float) event.altitudeM())
                        .setFloat("temp_external_c", (float) event.tempExternalC())
                        .setFloat("temp_internal_c", (float) event.tempInternalC())
                        .setString("text_payload", event.textPayload())
                        .build()),
                session.executeAsync(insertHistory.boundStatementBuilder()
                        .setConsistencyLevel(ConsistencyLevel.QUORUM)
                        .setString("entity_id", event.entityId())
                        .setInstant("event_time", event.eventTime())
                        .setUuid("event_id", event.eventId())
                        .setString("event_type", event.eventType())
                        .setString("observer_id", event.observerId())
                        .setDouble("latitude", event.latitude())
                        .setDouble("longitude", event.longitude())
                        .setFloat("altitude_m", (float) event.altitudeM())
                        .setFloat("temp_external_c", (float) event.tempExternalC())
                        .setFloat("temp_internal_c", (float) event.tempInternalC())
                        .setDouble("speed_mps", derived.speedMps())
                        .setDouble("heading_deg", derived.headingDeg())
                        .setString("zone_id", proximity.nearestZoneId())
                        .setString("text_payload", event.textPayload())
                        .build()),
                session.executeAsync(upsertLatest.boundStatementBuilder()
                        .setConsistencyLevel(ConsistencyLevel.QUORUM)
                        .setUuid("event_id", event.eventId())
                        .setInstant("event_time", event.eventTime())
                        .setString("event_type", event.eventType())
                        .setString("observer_id", event.observerId())
                        .setDouble("latitude", event.latitude())
                        .setDouble("longitude", event.longitude())
                        .setFloat("altitude_m", (float) event.altitudeM())
                        .setFloat("temp_external_c", (float) event.tempExternalC())
                        .setFloat("temp_internal_c", (float) event.tempInternalC())
                        .setDouble("speed_mps", derived.speedMps())
                        .setDouble("heading_deg", derived.headingDeg())
                        .setBoolean("is_flying", derived.flying())
                        .setInt("telemetry_age_s", telemetryAge)
                        .setBoolean("near_restricted_zone", proximity.nearZone())
                        .setBoolean("predicted_zone_breach", proximity.predictedBreach())
                        .setDouble("risk_score", proximity.riskScore())
                        .setString("text_payload", event.textPayload())
                        .setInstant("updated_at", now)
                        .setString("entity_id", event.entityId())
                        .build()));
    }

    @Override
    public void count(int records, String bucket) {
        session.executeAsync(countIngested.boundStatementBuilder()
                .setLong("record_count", records)
                .setString("bucket", bucket)
                .build());
    }

    @Override
    public void alert(AlertRow alert) {
        try {
            session.executeAsync(insertAlert.boundStatementBuilder()
                    .setString("bucket", alert.bucket())
                    .setInstant("alert_time", alert.alertTime())
                    .setString("entity_id", alert.entityId())
                    .setUuid("alert_id", alert.alertId())
                    .setString("alert_type", alert.alertType())
                    .setString("severity", alert.severity())
                    .setString("zone_id", alert.zoneId())
                    .setDouble("latitude", alert.latitude())
                    .setDouble("longitude", alert.longitude())
                    .setFloat("altitude_m", (float) alert.altitudeM())
                    .setString("message", alert.message())
                    .setDouble("risk_score", alert.riskScore())
                    .build());
        } catch (RuntimeException e) {
            Log.alert("could not write alert for %s: %s", alert.entityId(), e);
        }
    }

    /**
     * How far behind the reading is by the time it is written, in whole seconds.
     *
     * <p>Never negative: a producer whose clock is ahead of the sink's would otherwise write a
     * negative age into a column the dashboard reads as staleness.
     */
    static int telemetryAgeSeconds(Instant eventTime, Instant now) {
        return (int) Math.max(0, Duration.between(eventTime, now).toSeconds());
    }
}
