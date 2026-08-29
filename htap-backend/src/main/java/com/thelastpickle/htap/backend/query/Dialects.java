package com.thelastpickle.htap.backend.query;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The rewriting the five paths share, so each path's own {@code dialect} is a line or two.
 *
 * <p>What differs between the paths is the name a table is reachable under and whether the
 * engine accepts {@code ALLOW FILTERING}, which is Cassandra's alone. Nothing here parses SQL:
 * a statement is one {@code SELECT} the console validated, and these are the two textual
 * changes that make the same question run five ways.
 */
public final class Dialects {

    /**
     * The tables the console exposes.
     *
     * <p>A name not on this list is left alone, so a statement naming some other table reaches
     * the engine unrewritten and is refused by it. That is the intended answer: the list is
     * what the demo's five paths all register, and the refusal names the table.
     */
    public static final List<String> DEMO_TABLES = List.of(
            "drone_latest_status",
            "drone_events_by_entity",
            "drone_text_embeddings",
            "alerts_by_bucket",
            "ingestion_counts",
            "restricted_zones",
            "events");

    /**
     * A table reference, which is a name after {@code FROM} or {@code JOIN} and nowhere else.
     *
     * <p>Matching the bare word anywhere would rewrite whatever happened to share a table's
     * name: {@code SELECT count(*) AS events FROM events} would have its alias rewritten too,
     * and the engines would then disagree about what the result column is called. A
     * comma-separated table list is not handled, and no query the demo asks needs one.
     */
    private static final Pattern TABLE_REFERENCE = Pattern.compile(
            "(?<lead>\\b(?:FROM|JOIN)\\s+)(?:demo\\.)?(?<table>"
                    + String.join("|", DEMO_TABLES) + ")\\b",
            Pattern.CASE_INSENSITIVE);

    private static final Pattern ALLOW_FILTERING =
            Pattern.compile("\\s*ALLOW\\s+FILTERING\\s*", Pattern.CASE_INSENSITIVE);

    private static final Pattern TRAILING_LIMIT =
            Pattern.compile("\\s+LIMIT\\s+\\d+\\s*$", Pattern.CASE_INSENSITIVE);

    private Dialects() {}

    /** Every table reference as {@code <prefix><table>}, with any keyspace dropped. */
    public static String rewriteTables(String sql, String prefix) {
        Matcher matcher = TABLE_REFERENCE.matcher(sql);
        return matcher.replaceAll(match -> match.group("lead") + prefix + match.group("table"));
    }

    /** The statement with Cassandra's own clause removed, for the four engines without it. */
    public static String withoutAllowFiltering(String sql) {
        return ALLOW_FILTERING.matcher(sql).replaceAll(" ").strip();
    }

    /** The statement with its trailing {@code LIMIT n} removed. */
    public static String withoutLimit(String sql) {
        return TRAILING_LIMIT.matcher(sql).replaceAll("").strip();
    }

    /** Whether the statement already bounds itself. */
    public static boolean hasLimit(String sql) {
        return TRAILING_LIMIT.matcher(sql).find();
    }

    /** The statement bounded, leaving a bound the caller wrote in place. */
    public static String bounded(String sql, int limit) {
        return hasLimit(sql) ? sql : sql + " LIMIT " + limit;
    }

    /**
     * The four SQL engines' shared rewriting: drop the Cassandra clause, aim the table names
     * at {@code prefix}, and bound the result.
     */
    public static String sql(String statement, String prefix, int limit) {
        return bounded(rewriteTables(withoutAllowFiltering(statement), prefix), limit);
    }

    /**
     * The CQL spelling, which the other four do not share.
     *
     * <p>CQL wants {@code LIMIT n ALLOW FILTERING} in that order and knows no keyspace beyond
     * the session's own, so a bound the caller wrote is taken off and re-appended rather than
     * left where it was. Always bounded, and always filtering: a console statement over a
     * partitioned table needs the clause, and the refusals worth seeing are the ones the clause
     * does not rescue.
     */
    public static String cql(String statement, int limit) {
        String stripped = withoutLimit(withoutAllowFiltering(statement));
        return rewriteTables(stripped, "") + " LIMIT " + limit + " ALLOW FILTERING";
    }
}
