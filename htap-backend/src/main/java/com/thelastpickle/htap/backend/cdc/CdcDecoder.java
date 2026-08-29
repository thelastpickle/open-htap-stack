package com.thelastpickle.htap.backend.cdc;

import com.thelastpickle.htap.backend.api.dto.CdcRecord;
import com.thelastpickle.htap.backend.support.Messages;
import jakarta.enterprise.context.ApplicationScoped;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;

/**
 * One Kafka record turned into one mutation the page can show.
 *
 * <p>The framing is Confluent's: byte 0 is a magic byte, then the four-byte id of the schema the
 * registry holds, big-endian, then the Avro record. So a record cannot be read without the registry,
 * and a registry that cannot be reached is reported per record rather than dropping it.
 *
 * <p>The publisher wraps every row in an envelope, and the row itself is one nested Avro record
 * named {@code payload}. It is flattened here, because a column is what the page shows and
 * {@code payload.speed_mps} would be the envelope's shape rather than the table's. A record carrying
 * no {@code payload} keeps whatever else it had, so a schema change shows rather than hides.
 */
@ApplicationScoped
public class CdcDecoder {

    static final int MAGIC_BYTE = 0;
    static final int HEADER_BYTES = 5;

    /** The envelope's own fields, from {@code cdc_generic_record.avsc}. */
    static final Set<String> ENVELOPE_FIELDS = Set.of(
            "timestampMicros",
            "sourceTable",
            "sourceKeyspace",
            "truncatedFields",
            "version",
            "operationType",
            "isPartial",
            "updateFields",
            "range",
            "ttl",
            "payload");

    /**
     * The data model the records are read with.
     *
     * <p>The conversions are what make a {@code timestamp-millis} arrive as an {@link
     * java.time.Instant} rather than as a {@code long}: without them the page would show the number
     * the wire carries, where the Python's decoder converted by its logical type.
     */
    private static final GenericData MODEL = model();

    private final SchemaRegistry registry;

    CdcDecoder(SchemaRegistry registry) {
        this.registry = registry;
    }

    /**
     * One arrival, decoded as far as it can be.
     *
     * @param backfill whether the record was read to fill the buffer rather than seen arrive, which
     *     is what excludes it from the latency samples
     */
    CdcRecord decode(long seq, Arrival arrival, boolean backfill) {
        String key = arrival.key() == null ? "" : new String(arrival.key(), StandardCharsets.UTF_8);
        // The key is keyspace:table:hash and needs no schema, so a record whose value cannot be
        // read still says what it touched.
        String[] parts = key.split(":");
        String keyspace = parts.length >= 2 ? parts[0] : "";
        String table = parts.length >= 2 ? parts[1] : "";

        byte[] value = arrival.value() == null ? new byte[0] : arrival.value();
        Integer schemaId = null;
        GenericRecord decoded;
        try {
            if (value.length < HEADER_BYTES || value[0] != MAGIC_BYTE) {
                throw new IllegalArgumentException("not Confluent-framed: " + value.length
                        + " bytes beginning " + (value.length == 0 ? "nothing" : hex(value[0])));
            }
            schemaId = schemaId(value);
            decoded = read(value, registry.schema(schemaId));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return failed(seq, arrival, key, keyspace, table, backfill, schemaId, "interrupted");
        } catch (Exception e) {
            // One unreadable record must not stop the tail, and it stays in the buffer with its
            // reason: a record the dashboard cannot read is a finding.
            return failed(seq, arrival, key, keyspace, table, backfill, schemaId,
                    e.getClass().getSimpleName() + ": " + Messages.oneLine(e.getMessage()));
        }

        long mutationAtMs = writtenAtMs(decoded);
        return new CdcRecord(
                seq,
                arrival.partition(),
                arrival.offset(),
                key,
                text(decoded, "sourceKeyspace", keyspace),
                text(decoded, "sourceTable", table),
                text(decoded, "operationType", ""),
                mutationAtMs,
                arrival.timestampMs(),
                age(arrival.timestampMs(), mutationAtMs, backfill),
                backfill,
                Boolean.TRUE.equals(decoded.get("isPartial")),
                columns(decoded),
                updateFields(decoded),
                schemaId,
                null);
    }

    /**
     * The publisher's own delay for this record: the age at Kafka append.
     *
     * <p>Both figures ride on the record, so the field stays true while this backend's consumer is
     * behind. It differs from the Python, which subtracted the mutation time from the clock at
     * decode and so measured the consumer's own backlog as well; the page's label and CLAUDE.md
     * both describe the figure computed here.
     */
    private static Double age(long kafkaAtMs, long mutationAtMs, boolean backfill) {
        if (backfill || kafkaAtMs <= 0 || mutationAtMs <= 0) {
            return null;
        }
        return (double) (kafkaAtMs - mutationAtMs);
    }

    private CdcRecord failed(
            long seq,
            Arrival arrival,
            String key,
            String keyspace,
            String table,
            boolean backfill,
            Integer schemaId,
            String error) {
        return new CdcRecord(
                seq,
                arrival.partition(),
                arrival.offset(),
                key,
                keyspace,
                table,
                "",
                0,
                arrival.timestampMs(),
                null,
                backfill,
                false,
                Map.of(),
                List.of(),
                schemaId,
                error);
    }

    static int schemaId(byte[] framed) {
        return ((framed[1] & 0xFF) << 24)
                | ((framed[2] & 0xFF) << 16)
                | ((framed[3] & 0xFF) << 8)
                | (framed[4] & 0xFF);
    }

    private static GenericRecord read(byte[] framed, Schema schema) throws java.io.IOException {
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema, schema, MODEL);
        return reader.read(
                null,
                DecoderFactory.get()
                        .binaryDecoder(framed, HEADER_BYTES, framed.length - HEADER_BYTES, null));
    }

    /** The table's own columns, from {@code payload} where there is one. */
    private static Map<String, Object> columns(GenericRecord decoded) {
        if (decoded.get("payload") instanceof GenericRecord payload) {
            return CdcValues.fields(payload);
        }
        Map<String, Object> columns = new LinkedHashMap<>();
        decoded.getSchema().getFields().stream()
                .filter(field -> !ENVELOPE_FIELDS.contains(field.name()))
                .forEach(field -> columns.put(field.name(), CdcValues.jsonSafe(decoded.get(field.pos()))));
        return columns;
    }

    private static List<String> updateFields(GenericRecord decoded) {
        List<String> named = new ArrayList<>();
        if (decoded.get("updateFields") instanceof Iterable<?> fields) {
            fields.forEach(field -> named.add(String.valueOf(field)));
        }
        return named;
    }

    /** A field's value as text, or the fallback where the record does not carry one. */
    private static String text(GenericRecord decoded, String field, String fallback) {
        Object value = decoded.get(field);
        return value == null || value.toString().isEmpty() ? fallback : value.toString();
    }

    /** The mutation's own write time in milliseconds, from the envelope's microsecond field. */
    private static long writtenAtMs(GenericRecord decoded) {
        return switch (decoded.get("timestampMicros")) {
            case Number micros -> micros.longValue() / 1000;
            case Instant at -> at.toEpochMilli();
            case null, default -> 0L;
        };
    }

    private static String hex(byte first) {
        return String.format("%02x", first);
    }

    private static GenericData model() {
        GenericData data = new GenericData();
        data.addLogicalTypeConversion(new Conversions.DecimalConversion());
        data.addLogicalTypeConversion(new Conversions.UUIDConversion());
        data.addLogicalTypeConversion(new TimeConversions.DateConversion());
        data.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
        data.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        data.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        data.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
        return data;
    }
}
