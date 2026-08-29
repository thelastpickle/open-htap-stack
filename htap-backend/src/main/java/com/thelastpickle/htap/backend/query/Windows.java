package com.thelastpickle.htap.backend.query;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.thelastpickle.htap.backend.config.EventSettings;
import com.thelastpickle.htap.backend.engine.CassandraPath;
import com.thelastpickle.htap.common.EventPartitions;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DecimalStyle;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.jboss.logging.Logger;

/**
 * Which event window the compare page should ask about.
 *
 * <p>Chosen here rather than in the browser because the answer depends on the data: the buckets
 * exist only because the sink wrote them, so it is the stack's clock, its configuration and its
 * contents that decide which one is worth naming. A demo minutes old has no closed window holding
 * anything, since the first quarter of an hour after a wipe is all in the window still filling.
 */
@ApplicationScoped
public class Windows {

    /**
     * How far back to look for a window holding events.
     *
     * <p>Bounded because the search is one read per window and the answer is wanted while a page
     * loads. Two hours is far more than a demo that has been ingesting needs, and a demo that has
     * not been ingesting for two hours has nothing to show anyway.
     */
    static final int LOOKBACK = 8;

    private static final Logger LOG = Logger.getLogger(Windows.class);

    private static final DateTimeFormatter BUCKET_FORMAT = DateTimeFormatter
            .ofPattern("uuuu-MM-dd'T'HH:mm")
            .withDecimalStyle(DecimalStyle.STANDARD);

    private final EventSettings events;
    private final CassandraPath cassandra;
    private final SinkProgress progress;

    @Inject
    Windows(EventSettings events, CassandraPath cassandra, SinkProgress progress) {
        this.events = events;
        this.cassandra = cassandra;
        this.progress = progress;
    }

    /** The newest closed window holding events, or the one now filling when there is none. */
    public WindowChoice choose() {
        return choose(Instant.now(), this::holdsEvents, progress::consumedPast);
    }

    /**
     * The choice, given a clock, a way to ask whether a window holds anything, and a way to ask
     * how far the sink has got.
     *
     * <p>The two lookups are parameters so that the walk backwards can be tested without a
     * cluster or a broker: what is worth pinning is which window is named and what is claimed of
     * it, and both are decided here rather than by either engine.
     */
    WindowChoice choose(
            Instant now,
            Predicate<String> holdsEvents,
            Function<Instant, SinkProgress.Verdict> settled) {
        int minutes = events.bucketMinutes();
        String current = EventPartitions.bucket(now, minutes);
        for (int step = 1; step <= LOOKBACK; step++) {
            Instant stepped = now.minus(Duration.ofMinutes((long) minutes * step));
            String candidate = EventPartitions.bucket(stepped, minutes);
            if (!holdsEvents.test(candidate)) {
                continue;
            }
            Instant end = startOf(candidate).plus(Duration.ofMinutes(minutes));
            SinkProgress.Verdict verdict = settled.apply(end);
            return new WindowChoice(minutes, events.shards(), current, candidate, true,
                    verdict.settled(), verdict.detail());
        }
        return new WindowChoice(minutes, events.shards(), current, current, false, false,
                // The window still filling is by definition still being written to.
                "the window is still filling");
    }

    /**
     * The instant a window began, read back from its own label.
     *
     * <p>Parsed rather than computed a second time: the label is what the sink wrote and what the
     * five paths are asked about, so deriving the window's end from anything else could name a
     * different span than the one being read. This is the inverse of {@link
     * EventPartitions#bucket}.
     */
    static Instant startOf(String bucket) {
        return LocalDateTime.parse(bucket, BUCKET_FORMAT).toInstant(ZoneOffset.UTC);
    }

    /**
     * A shard count no request could make valid is refused once, at startup.
     *
     * <p>Said here as well as in {@link #holdsEvents} because this class is the one that cannot work
     * without it, and because the route is a {@code GET}: an {@code IllegalArgumentException} raised
     * from a request reaches the browser as an unmapped 500 with no {@code detail} for the compare
     * page to show, where a startup failure names the setting in the log the operator is reading.
     */
    void onStart(@Observes StartupEvent event) {
        refuseZeroShards();
    }

    /**
     * Refused rather than clamped, as {@link EventPartitions#shard} refuses the same value.
     *
     * <p>A count of zero makes the shard list empty, and {@code shard IN ()} is a
     * {@code SyntaxException} that {@link #holdsEvents}'s own catch would swallow once per candidate
     * window, so a misconfigured stack would report eight warnings and no window rather than the one
     * thing wrong with it.
     */
    private void refuseZeroShards() {
        if (events.shards() < 1) {
            throw new IllegalArgumentException(
                    "EVENT_SHARDS must be at least 1, got " + events.shards());
        }
    }

    /**
     * Whether any shard of this window has a row in it.
     *
     * <p>One read of the window's partitions, stopped at the first row, so the cost does not
     * depend on how full the window is. A window whose contents cannot be read is treated as
     * empty, which makes the caller fall through to the one now filling rather than fail.
     */
    boolean holdsEvents(String bucket) {
        refuseZeroShards();
        String shards = IntStream.range(0, events.shards())
                .mapToObj(Integer::toString)
                .collect(Collectors.joining(","));
        // The bucket is bound and the shard list interpolated: the first is a value, and the
        // second is a list whose length is the configured shard count rather than one value.
        SimpleStatement statement = SimpleStatement.newInstance(
                "SELECT event_bucket FROM events WHERE event_bucket = ? AND shard IN ("
                        + shards + ") LIMIT 1",
                bucket);
        try {
            return cassandra.execute(statement).one() != null;
        } catch (RuntimeException e) {
            LOG.warnf("could not check window %s for events: %s", bucket, e);
            return false;
        }
    }
}
