package com.thelastpickle.htap.backend.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.thelastpickle.htap.backend.api.dto.CdcPayloadColumn;
import com.thelastpickle.htap.backend.api.dto.CdcSchemaField;
import com.thelastpickle.htap.backend.api.dto.CdcSchemaView;
import com.thelastpickle.htap.backend.config.CdcSettings;
import com.thelastpickle.htap.backend.support.Messages;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import org.jboss.logging.Logger;

/**
 * The contract the topic's records are written against, read from the registry.
 *
 * <p>From the registry and not from a record, because the point is that the contract lives there:
 * {@code {topic}-value} is the subject name a Confluent serializer registers under, and Apicurio
 * serves it through its compatibility endpoint.
 */
@ApplicationScoped
public class CdcContract {

    private static final Logger LOG = Logger.getLogger(CdcContract.class);

    private final SchemaRegistry registry;
    private final CdcSettings settings;
    private final ObjectMapper json;

    CdcContract(SchemaRegistry registry, CdcSettings settings, ObjectMapper json) {
        this.registry = registry;
        this.settings = settings;
        this.json = json;
    }

    /** The latest registered version, or why it could not be read. */
    public CdcSchemaView published() {
        String subject = settings.subject();
        String where = settings.registry();
        long startedAt = System.nanoTime();
        try {
            JsonNode body = registry.latest(subject);
            if (body == null) {
                return CdcSchemaView.failed(subject, where, "no schema registered for " + subject
                        + " yet; the Sidecar registers one with its first published mutation");
            }
            JsonNode schema = json.readTree(body.path("schema").asText());
            return new CdcSchemaView(
                    subject,
                    number(body, "id"),
                    number(body, "version"),
                    fields(schema),
                    payloadColumns(schema),
                    where,
                    schema,
                    null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return CdcSchemaView.failed(subject, where, "interrupted while asking the registry");
        } catch (Exception e) {
            // The page reports a registry it cannot reach, since the tail can be running without it.
            //
            // Logged as well as returned, with how long the attempt took.  Returning it alone put
            // the cause in an HTTP response body and nowhere else, so a CI run that failed on
            // SchemaRegistry.TIMEOUT showed three consequences in its annotations and no elapsed
            // time to say whether the registry had been slow or absent.
            String why = e.getClass().getSimpleName() + ": " + Messages.oneLine(e.getMessage());
            LOG.warnf("the registry at %s did not serve %s after %d ms: %s",
                    where, subject, (System.nanoTime() - startedAt) / 1_000_000L, why);
            return CdcSchemaView.failed(subject, where, why);
        }
    }

    /** The envelope's own fields, in declaration order, with their types as declared. */
    static List<CdcSchemaField> fields(JsonNode schema) {
        List<CdcSchemaField> fields = new ArrayList<>();
        for (JsonNode field : schema.path("fields")) {
            fields.add(new CdcSchemaField(field.path("name").asText(null), field.get("type")));
        }
        return fields;
    }

    /**
     * The table's own columns, out of the envelope's nested {@code payload} record.
     *
     * <p>Each column is a union of one type and null, and the publisher writes the CQL type it
     * converted from onto the Avro type as a {@code cqlType} property. Reading it back is what lets
     * the page say {@code timestamp} where Avro says {@code long}.
     */
    static List<CdcPayloadColumn> payloadColumns(JsonNode schema) {
        for (JsonNode field : schema.path("fields")) {
            if (!"payload".equals(field.path("name").asText())) {
                continue;
            }
            JsonNode nested = field.get("type");
            if (nested == null || !nested.isObject()) {
                return List.of();
            }
            List<CdcPayloadColumn> columns = new ArrayList<>();
            for (JsonNode column : nested.path("fields")) {
                JsonNode declared = declaredBranch(column.get("type"));
                columns.add(new CdcPayloadColumn(
                        column.path("name").asText(null),
                        declared.hasNonNull("type")
                                ? declared.path("type").asText()
                                : declared.path("logicalType").asText(null),
                        declared.path("cqlType").asText(null)));
            }
            return columns;
        }
        return List.of();
    }

    /** The branch of a union that says something: the null branch is spelled as a bare string. */
    private static JsonNode declaredBranch(JsonNode type) {
        if (type == null) {
            return MissingNode.getInstance();
        }
        if (type.isArray()) {
            for (JsonNode branch : type) {
                if (branch.isObject()) {
                    return branch;
                }
            }
            return MissingNode.getInstance();
        }
        return type.isObject() ? type : MissingNode.getInstance();
    }

    private static Integer number(JsonNode body, String field) {
        JsonNode value = body.get(field);
        return value == null || !value.isNumber() ? null : value.intValue();
    }
}
