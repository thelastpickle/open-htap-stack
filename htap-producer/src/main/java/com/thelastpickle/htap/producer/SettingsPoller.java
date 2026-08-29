package com.thelastpickle.htap.producer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;

/**
 * Copies the dashboard's demo controls in, on its own thread.
 *
 * <p>Every failure is a reason to keep the current values rather than to stop: a backend that is
 * not running yet, one restarting and one answering rubbish are all ordinary states here. A
 * repeated failure is logged once, so a stack with no dashboard does not fill the log with one
 * line every ten seconds.
 */
final class SettingsPoller implements Runnable {

    /** Short, because the poll is off the send path and a slow backend must not hold a thread. */
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private static final JsonFactory JSON = new JsonFactory();

    /** One GET of the settings route, so what the backend said can be scripted in a test. */
    interface Fetch {
        String get(String url) throws IOException;
    }

    private final String url;
    private final LiveSettings live;
    private final Duration interval;
    private final Fetch fetch;
    private final Log log;

    private volatile boolean running = true;
    private String lastError = "";

    SettingsPoller(String url, LiveSettings live, Duration interval, Fetch fetch, Log log) {
        this.url = url;
        this.live = live;
        this.interval = interval;
        this.fetch = fetch;
        this.log = log;
    }

    static SettingsPoller overHttp(String url, LiveSettings live, Duration interval, Log log) {
        return new SettingsPoller(url, live, interval, Http.get(TIMEOUT), log);
    }

    void stop() {
        running = false;
    }

    @Override
    public void run() {
        while (running) {
            if (!sleepOneInterval()) {
                return;
            }
            poll();
        }
    }

    /** One poll, which either adopts what the backend said or keeps what this process had. */
    void poll() {
        try {
            LiveSettings.Applied applied = live.apply(parse(fetch.get(url)));
            lastError = "";
            if (applied.changed()) {
                LiveSettings.Snapshot now = applied.snapshot();
                log.say(String.format(
                        Locale.ROOT,
                        "settings updated: eps=%d n_entities=%d outlier_percent=%s paused=%s",
                        now.eventsPerSec(),
                        now.nEntities(),
                        now.outlierPercent(),
                        now.paused()));
            }
        } catch (IOException | RuntimeException e) {
            String message = e.getClass().getSimpleName() + ": " + e.getMessage();
            if (!message.equals(lastError)) {
                log.say("settings poll failed, keeping current values (" + message + ")");
                lastError = message;
            }
        }
    }

    /**
     * The four controls out of the route's {@code settings} object.
     *
     * <p>Streamed rather than bound to a tree, and a field this does not know is skipped: the
     * route's body carries more than the producer acts on, and a new field there must not stop
     * the fleet.
     */
    static LiveSettings.Reported parse(String body) throws IOException {
        OptionalInt eventsPerSec = OptionalInt.empty();
        OptionalInt dronesEnabled = OptionalInt.empty();
        OptionalDouble outlierPercent = OptionalDouble.empty();
        Optional<Boolean> paused = Optional.empty();

        try (JsonParser json = JSON.createParser(body)) {
            if (json.nextToken() != JsonToken.START_OBJECT) {
                throw new IOException("the settings route answered no object");
            }
            while (json.nextToken() == JsonToken.FIELD_NAME) {
                boolean settings = "settings".equals(json.currentName());
                json.nextToken();
                if (!settings) {
                    json.skipChildren();
                    continue;
                }
                while (json.nextToken() == JsonToken.FIELD_NAME) {
                    String field = json.currentName();
                    json.nextToken();
                    switch (field) {
                        case "events_per_sec" -> eventsPerSec = OptionalInt.of(json.getIntValue());
                        case "drones_enabled" -> dronesEnabled = OptionalInt.of(json.getIntValue());
                        case "outlier_percent" ->
                                outlierPercent = OptionalDouble.of(json.getDoubleValue());
                        case "paused" -> paused = Optional.of(json.getBooleanValue());
                        default -> json.skipChildren();
                    }
                }
            }
        }
        return new LiveSettings.Reported(eventsPerSec, dronesEnabled, outlierPercent, paused);
    }

    private boolean sleepOneInterval() {
        try {
            Thread.sleep(interval);
            return running;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
