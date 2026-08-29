package com.thelastpickle.htap.backend.query;

import com.thelastpickle.htap.backend.engine.QueryPath;
import com.thelastpickle.htap.backend.support.Messages;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * Rebuilding this backend's client for a path, and saying what that took.
 *
 * <p>The client is restarted, not the service. The dashboard is a container beside the others with no
 * control over them, which is the right way round for something reachable from a browser; restarting
 * a service is a command on the host, and the page shows which one. The reconnect is the useful half
 * anyway: it is what clears a session that has gone stale, and it costs no downtime.
 */
@ApplicationScoped
public class Reconnection {

    private final QueryPaths paths;

    @Inject
    Reconnection(QueryPaths paths) {
        this.paths = paths;
    }

    /** Every path's name, which is what {@code all} means and what a target is checked against. */
    public List<String> targets() {
        return paths.all().stream().map(QueryPath::name).toList();
    }

    /**
     * Rebuild one path or all of them, one line per path.
     *
     * @return the lines, and whether every path asked for is now connected
     */
    public Outcome reconnect(List<String> names) {
        List<String> actions = new ArrayList<>();
        int reconnected = 0;
        for (String name : names) {
            QueryPath path = paths.byName(name).orElseThrow();
            if (path.busy()) {
                // Connecting would queue behind the statement rather than replace it, and a control
                // that hangs for a quarter of an hour explains nothing.
                actions.add(name + ": busy with a query, so stop that first");
                continue;
            }
            try {
                // Forced, because a path's own connect is otherwise a no-op when it already believes
                // it is connected, which is the state a stale session is in.
                path.connect(true);
            } catch (RuntimeException e) {
                actions.add(name + ": " + Messages.oneLine(e));
                continue;
            }
            if (path.connected()) {
                reconnected++;
                actions.add(name + ": reconnected");
            } else {
                actions.add(name + ": still unreachable");
            }
        }
        return new Outcome(reconnected == names.size(), actions);
    }

    /** Whether every path asked for came back, and the line each of them produced. */
    public record Outcome(boolean ok, List<String> actions) {}
}
