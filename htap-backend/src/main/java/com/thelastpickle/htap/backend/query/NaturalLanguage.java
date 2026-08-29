package com.thelastpickle.htap.backend.query;

import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A question in words turned into SQL, with no model involved.
 *
 * <p>The rules are small on purpose: they exist so the page works with no API key and no network,
 * and a translator that cannot be reached falls back here rather than failing the request. What the
 * rules produce is Presto SQL, because a question in words asks for ordering, ranges and aggregates,
 * which are ordinary SQL and are the things CQL cannot answer; Presto reads the same live Cassandra
 * rows, so the answer is still current.
 */
public final class NaturalLanguage {

    /** The path a translated statement runs on, for the reason above. */
    public static final String ENGINE = "presto";

    /** The one table either translator may name, and the whole of what the model is told about it. */
    public static final String SCHEMA =
            "demo.drone_latest_status(entity_id, event_time, latitude, longitude, altitude_m, "
                    + "speed_mps, heading_deg, is_flying, temp_internal_c, temp_external_c, "
                    + "near_restricted_zone, predicted_zone_breach, risk_score)";

    private static final String BASE =
            "SELECT entity_id, event_time, latitude, longitude, altitude_m, speed_mps, "
                    + "temp_internal_c, risk_score FROM demo.drone_latest_status";

    /**
     * A comparison and the one or two numbers it names.
     *
     * <p>The second number is what tells "between 10 and 20" from "above 10", and it is optional in
     * the pattern rather than in a second pattern, so a phrase is read once.
     */
    private static final Pattern COMPARISON = Pattern.compile(
            "(above|over|greater than|more than|below|less than|under|between)\\s+"
                    + "(-?\\d+(?:\\.\\d+)?)(?:\\s*(?:and|to|-)\\s*(-?\\d+(?:\\.\\d+)?))?");

    /** Which column a question is about, by the words a viewer uses for it. */
    private static final List<Measure> MEASURES = List.of(
            new Measure(List.of("temperature", "temp", "hot", "overheat"), "temp_internal_c"),
            new Measure(List.of("altitude", "height", "high"), "altitude_m"),
            new Measure(List.of("speed", "fast", "velocity"), "speed_mps"),
            new Measure(List.of("risk"), "risk_score"));

    private static final List<String> COUNTING =
            List.of("count", "how many", "total", "stats");

    private NaturalLanguage() {}

    /** The statement a prompt asks for, in whatever spelling it arrived. */
    public static String toSql(String prompt) {
        String asked = prompt.toLowerCase(Locale.ROOT);
        String measure = measure(asked);
        if (measure != null) {
            return bounded(asked, measure);
        }
        if (asked.contains("breach")) {
            return BASE + " WHERE predicted_zone_breach = true ORDER BY risk_score DESC";
        }
        if (asked.contains("zone")) {
            return BASE + " WHERE near_restricted_zone = true ORDER BY risk_score DESC";
        }
        if (asked.contains("ground")) {
            return BASE + " WHERE is_flying = false";
        }
        if (mentions(asked, List.of("flying", "active", "airborne"))) {
            return BASE + " WHERE is_flying = true";
        }
        if (mentions(asked, COUNTING)) {
            return "SELECT count(*) AS assets, count_if(is_flying) AS flying, "
                    + "count_if(near_restricted_zone) AS near_zone, "
                    + "round(avg(speed_mps), 1) AS avg_speed_mps FROM demo.drone_latest_status";
        }
        return BASE;
    }

    /**
     * How the page should draw the answer.
     *
     * <p>Read from the question rather than from the rows, because the same columns are a table or a
     * map depending on what was asked: "where are the drones" and "list the drones" both answer with
     * latitudes.
     */
    public static String renderHint(String prompt) {
        String asked = prompt.toLowerCase(Locale.ROOT);
        if (mentions(asked, List.of("map", "where", "location", "position"))) {
            return "map";
        }
        if (mentions(asked, COUNTING)) {
            return "kpi";
        }
        if (mentions(asked, List.of("trend", "history", "over time"))) {
            return "chart";
        }
        return "table";
    }

    /** A measure with a comparison is a range; a measure alone is an ordering. */
    private static String bounded(String asked, String measure) {
        Matcher comparison = COMPARISON.matcher(asked);
        if (!comparison.find()) {
            return BASE + " ORDER BY " + measure + " DESC";
        }
        String operator = comparison.group(1);
        String first = comparison.group(2);
        String second = comparison.group(3);
        if ("between".equals(operator) && second != null) {
            return BASE + " WHERE " + measure + " BETWEEN " + first + " AND " + second
                    + " ORDER BY " + measure + " DESC";
        }
        if (List.of("above", "over", "greater than", "more than").contains(operator)) {
            return BASE + " WHERE " + measure + " > " + first + " ORDER BY " + measure + " DESC";
        }
        return BASE + " WHERE " + measure + " < " + first + " ORDER BY " + measure + " ASC";
    }

    private static String measure(String asked) {
        return MEASURES.stream()
                .filter(measure -> mentions(asked, measure.words()))
                .map(Measure::column)
                .findFirst()
                .orElse(null);
    }

    private static boolean mentions(String asked, List<String> words) {
        return words.stream().anyMatch(asked::contains);
    }

    private record Measure(List<String> words, String column) {}
}
