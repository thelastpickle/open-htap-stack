package com.thelastpickle.htap.backend.query;

import com.thelastpickle.htap.backend.read.CassandraReads;
import com.thelastpickle.htap.backend.read.FleetRow;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import org.jboss.logging.Logger;

/**
 * The comparison's probe read: one asset out of {@code drone_latest_status}, read by partition key.
 *
 * <p>The single-partition read Cassandra is here for, so what the probe measures is the request path
 * itself rather than anything the demo arranged for the purpose.
 */
@ApplicationScoped
public class FleetSampler implements OltpSampler {

    private static final Logger LOG = Logger.getLogger(FleetSampler.class);

    private final CassandraReads reads;

    @Inject
    FleetSampler(CassandraReads reads) {
        this.reads = reads;
    }

    @Override
    public Optional<String> subject() {
        try {
            List<FleetRow> found = reads.drones(1, false);
            return found.isEmpty() ? Optional.empty() : Optional.of(found.getFirst().entityId());
        } catch (RuntimeException e) {
            LOG.debugf("no asset to probe: %s", e);
            return Optional.empty();
        }
    }

    @Override
    public OltpProbe sample(String entityId) {
        return OltpProbe.start(() -> reads.drone(entityId));
    }
}
