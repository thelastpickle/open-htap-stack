package com.thelastpickle.htap.backend.cdc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thelastpickle.htap.backend.api.dto.CdcPayloadColumn;
import com.thelastpickle.htap.backend.api.dto.CdcSchemaField;
import com.thelastpickle.htap.backend.api.dto.CdcSchemaView;
import com.thelastpickle.htap.backend.config.CdcSettings;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** What the page is told the contract is, read from the subject the publisher registers. */
class CdcContractTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final CdcSettings settings = CdcFixtures.settings();

    /** The envelope, as the registry declares it: eleven fields, in the publisher's own order. */
    @Test
    void theEnvelopesFieldsComeBackInDeclarationOrder() {
        List<CdcSchemaField> fields = CdcContract.fields(schemaNode());

        assertEquals(
                List.of(
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
                        "payload"),
                fields.stream().map(CdcSchemaField::name).toList());
        assertEquals("long", fields.getFirst().type().toString().replace("\"", ""));
    }

    /**
     * The table's columns, with the CQL type each was converted from.
     *
     * <p>Every column is a union of its type and null, and the declared branch comes first. Where it
     * carries a logical type the plain type is still reported, which is what the Python did and what
     * lets the page show both.
     */
    @Test
    void eachColumnCarriesTheCqlTypeItCameFrom() {
        Map<String, CdcPayloadColumn> columns = new LinkedHashMap<>();
        CdcContract.payloadColumns(schemaNode()).forEach(column -> columns.put(column.name(), column));

        assertEquals(19, columns.size());
        assertEquals(new CdcPayloadColumn("entity_id", "string", "text"), columns.get("entity_id"));
        assertEquals(new CdcPayloadColumn("event_id", "string", "timeuuid"), columns.get("event_id"));
        assertEquals(new CdcPayloadColumn("event_time", "long", "timestamp"), columns.get("event_time"));
        assertEquals(new CdcPayloadColumn("altitude_m", "float", "float"), columns.get("altitude_m"));
    }

    /** A schema with no {@code payload} has no columns to report, and says so rather than raising. */
    @Test
    void aSchemaWithNoPayloadReportsNoColumns() throws JsonProcessingException {
        JsonNode envelopeOnly = JSON.readTree("""
                {"type": "record", "name": "CassandraCDC", "fields": [
                  {"name": "timestampMicros", "type": "long"}]}""");

        assertEquals(List.of(), CdcContract.payloadColumns(envelopeOnly));
        assertEquals(
                List.of("timestampMicros"),
                CdcContract.fields(envelopeOnly).stream().map(CdcSchemaField::name).toList());
    }

    /** The whole view: the ids the registry gave, the schema itself, and no error. */
    @Test
    void thePublishedViewCarriesTheIdsAndTheSchema() {
        CdcSchemaView view = contract(reply(200, subjectBody())).published();

        assertEquals("cdc-mutations-value", view.subject());
        assertEquals(Integer.valueOf(1), view.schemaId());
        assertEquals(Integer.valueOf(1), view.version());
        assertEquals("http://apicurio:8080/apis/ccompat/v7", view.registry());
        assertEquals("CassandraCDC", view.avroSchema().path("name").asText());
        assertEquals(11, view.fields().size());
        assertEquals(19, view.payloadFields().size());
        assertNull(view.error());
    }

    /**
     * A subject nobody has registered is an ordinary state on a stack that is minutes old.
     *
     * <p>Reported as such rather than as a registry that failed: the subject appears with the
     * publisher's first mutation.
     */
    @Test
    void anUnregisteredSubjectSaysWhatWillRegisterIt() {
        CdcSchemaView view = contract(reply(404, "{\"error_code\": 40401}")).published();

        assertEquals(
                "no schema registered for cdc-mutations-value yet;"
                        + " the Sidecar registers one with its first published mutation",
                view.error());
        assertEquals(List.of(), view.fields());
        assertNull(view.schemaId());
    }

    /** Any other refusal names the status and the path, so the failure can be attributed. */
    @Test
    void aRefusedReadNamesTheStatusAndThePath() {
        CdcSchemaView view = contract(reply(500, "")).published();

        assertEquals(
                "IOException: the registry answered HTTP 500 for"
                        + " /apis/ccompat/v7/subjects/cdc-mutations-value/versions/latest",
                view.error());
    }

    /** A registry that cannot be reached at all is reported by type, since it carries no status. */
    @Test
    void anUnreachableRegistryIsReportedByItsFailure() {
        CdcSchemaView view = contract(url -> {
            throw new java.net.ConnectException("Connection refused");
        }).published();

        assertEquals("ConnectException: Connection refused", view.error());
        assertEquals("cdc-mutations-value", view.subject());
    }

    private CdcContract contract(SchemaRegistry.Http http) {
        return new CdcContract(new SchemaRegistry(settings, JSON, http), settings, JSON);
    }

    private static SchemaRegistry.Http reply(int status, String body) {
        return url -> new SchemaRegistry.Reply(status, body);
    }

    /** The registry's own reply shape: the schema as a string inside a JSON document. */
    private static String subjectBody() {
        try {
            return JSON.writeValueAsString(Map.of(
                    "subject", "cdc-mutations-value",
                    "id", 1,
                    "version", 1,
                    "schema", CdcFixtures.schema().toString()));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private static JsonNode schemaNode() {
        try {
            return JSON.readTree(CdcFixtures.schema().toString());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
