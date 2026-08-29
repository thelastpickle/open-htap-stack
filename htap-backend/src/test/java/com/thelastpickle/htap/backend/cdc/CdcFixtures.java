package com.thelastpickle.htap.backend.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thelastpickle.htap.backend.config.CdcSettings;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

/**
 * The publisher's own schema, and records written against it.
 *
 * <p>{@code cdc-mutations-value.avsc} is the schema the running stack's Apicurio holds at id 1, taken
 * from {@code /apis/ccompat/v7/subjects/cdc-mutations-value/versions/latest} rather than written here:
 * a fixture this port invented could agree with the port and disagree with the publisher. It is the
 * eleven-field {@code CassandraCDC} envelope around a nested {@code drone_latest_status} record of
 * nineteen columns, each column a union of its type and null carrying the CQL type it came from.
 */
final class CdcFixtures {

    static final int SCHEMA_ID = 1;

    private static final Schema SCHEMA = load();

    private CdcFixtures() {}

    static Schema schema() {
        return SCHEMA;
    }

    static Schema payloadSchema() {
        return SCHEMA.getField("payload").schema();
    }

    static CdcSettings settings() {
        return settings(200);
    }

    static CdcSettings settings(int bufferSize) {
        return new CdcSettings() {
            @Override
            public String topic() {
                return "cdc-mutations";
            }

            @Override
            public String schemaRegistryUrl() {
                return "http://apicurio:8080/apis/ccompat/v7/";
            }

            @Override
            public int bufferSize() {
                return bufferSize;
            }

            @Override
            public double pollTimeoutSeconds() {
                return 1.0;
            }
        };
    }

    /** A registry that holds this schema at {@link #SCHEMA_ID} and knows nothing else. */
    static SchemaRegistry registry(CdcSettings settings) {
        ObjectMapper json = new ObjectMapper();
        return new SchemaRegistry(settings, json, url -> {
            if (url.getPath().endsWith("/schemas/ids/" + SCHEMA_ID)) {
                return new SchemaRegistry.Reply(
                        200, json.writeValueAsString(Map.of("schema", SCHEMA.toString())));
            }
            return new SchemaRegistry.Reply(404, "{\"error_code\": 40403}");
        });
    }

    /** One mutation of the table, with the columns named and the rest null. */
    static GenericRecord mutation(String operation, long micros, Map<String, Object> columns) {
        GenericRecord payload = new GenericData.Record(payloadSchema());
        columns.forEach(payload::put);

        GenericRecord envelope = new GenericData.Record(SCHEMA);
        envelope.put("timestampMicros", micros);
        envelope.put("sourceKeyspace", "demo");
        envelope.put("sourceTable", "drone_latest_status");
        envelope.put("truncatedFields", List.of());
        envelope.put("version", null);
        envelope.put(
                "operationType",
                new GenericData.EnumSymbol(SCHEMA.getField("operationType").schema(), operation));
        envelope.put("isPartial", false);
        envelope.put("updateFields", List.copyOf(columns.keySet()));
        envelope.put("range", null);
        envelope.put("ttl", null);
        envelope.put("payload", payload);
        return envelope;
    }

    /** The columns of a plausible telemetry update, in the order the publisher declares them. */
    static Map<String, Object> telemetry() {
        Map<String, Object> columns = new LinkedHashMap<>();
        columns.put("entity_id", "drone-0007");
        columns.put("event_id", "8f14e45f-ceea-467a-9f38-0e5e2a5b0000");
        columns.put("event_time", 1787846133_000000L);
        columns.put("speed_mps", 12.5);
        columns.put("is_flying", true);
        columns.put("text_payload", "holding at waypoint 4");
        return columns;
    }

    /** The record in the Confluent framing the publisher writes: a magic byte, then the schema id. */
    static byte[] framed(int schemaId, GenericRecord record) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.write(CdcDecoder.MAGIC_BYTE);
        bytes.write(schemaId >>> 24);
        bytes.write(schemaId >>> 16);
        bytes.write(schemaId >>> 8);
        bytes.write(schemaId);
        try {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bytes, null);
            // The plain model, with no conversions: the publisher writes a uuid as a string and a
            // timestamp as a long, and the decoder's own model is what converts them back.
            new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }

    /** One arrival carrying this record, with the broker timestamp a caller wants to measure by. */
    static Arrival arrival(int partition, long offset, long kafkaAtMs, byte[] value) {
        return new Arrival(
                partition,
                offset,
                "demo:drone_latest_status:0007".getBytes(StandardCharsets.UTF_8),
                kafkaAtMs,
                value);
    }

    private static Schema load() {
        try (InputStream stream =
                CdcFixtures.class.getResourceAsStream("/cdc-mutations-value.avsc")) {
            return new Schema.Parser().parse(stream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
