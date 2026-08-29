package com.thelastpickle.htap.backend.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thelastpickle.htap.backend.config.CassandraSettings;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.OptionalLong;
import org.jboss.logging.Logger;

/**
 * The Cassandra Sidecar's HTTP interface: what a bulk read is about to read.
 *
 * <p>The bulk reader streams SSTable files from the Sidecar, so the volume of a read is knowable
 * exactly, being the size of the snapshot the reader just took. Reporting it is what tells a slow
 * read that is simply large from one that has gone wrong, and on a demo whose table grows by tens
 * of megabytes a minute that distinction is most of the question. Only the Sidecar can answer it:
 * Cassandra's own table size is a near figure rather than the same one, and Spark's metrics
 * describe the job.
 */
@ApplicationScoped
public class Sidecar {

    private static final Logger LOG = Logger.getLogger(Sidecar.class);

    /**
     * The bound on one sizing request.
     *
     * <p>This runs inside the read that a reported duration times, so whatever it waits is added
     * to a figure; it is a bound rather than a generous value. Five seconds was inside the range
     * the Sidecar actually takes: measured from one CI runner's access log, listing the files of
     * a three-SSTable snapshot of {@code demo.drone_latest_status} answered 200 after 3.75 s,
     * 3.45 s and 6.1 s. The third exceeded a 5 s timeout, the size came back absent, and the
     * dashboard step failed with "spark_bulk answered without saying how many bytes it scanned"
     * on a read that had otherwise returned its five rows. Fifteen is 2.5 times the worst of the
     * three, and a Sidecar slow enough to reach it is one whose read will be slow anyway.
     */
    static final Duration TIMEOUT = Duration.ofSeconds(15);

    private final CassandraSettings settings;
    private final ObjectMapper json;
    private final HttpClient http;

    @Inject
    Sidecar(CassandraSettings settings, ObjectMapper json) {
        this.settings = settings;
        this.json = json;
        this.http = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();
    }

    /**
     * The total size of every file in one snapshot, or absent when it cannot be read.
     *
     * <p>Every component counts and not only {@code Data.db}: the reader opens the index and
     * filter files too, and they are part of what the Sidecar streams.
     *
     * <p>Absent rather than thrown. This is a figure to report beside a result, so failing to get
     * it must not fail the read that is about to happen; the bulk path also reads an absent answer
     * as a snapshot that has gone, which is how it decides whether one can be read again.
     */
    public OptionalLong snapshotBytes(String table, String snapshot) {
        URI url = URI.create("http://%s:%d/api/v1/keyspaces/%s/tables/%s/snapshots/%s"
                .formatted(
                        settings.host(),
                        settings.sidecarPort(),
                        settings.keyspace(),
                        table,
                        snapshot));
        try {
            HttpRequest request = HttpRequest.newBuilder(url).timeout(TIMEOUT).GET().build();
            HttpResponse<String> response =
                    http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() / 100 != 2) {
                LOG.infof("could not size snapshot %s: HTTP %d", snapshot, response.statusCode());
                return OptionalLong.empty();
            }
            long total = 0;
            for (JsonNode file : json.readTree(response.body()).path("snapshotFilesInfo")) {
                total += file.path("size").asLong(0);
            }
            return OptionalLong.of(total);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return OptionalLong.empty();
        } catch (Exception e) {
            LOG.infof("could not size snapshot %s: %s", snapshot, e);
            return OptionalLong.empty();
        }
    }
}
