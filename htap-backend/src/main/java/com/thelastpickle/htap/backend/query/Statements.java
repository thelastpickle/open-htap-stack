package com.thelastpickle.htap.backend.query;

import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * What the console accepts: one read-only statement.
 *
 * <p>Every engine here is reachable read-write, and the console runs whatever it is given on the
 * engine the caller chose, so the restriction is the only thing between a browser and a {@code
 * TRUNCATE}. Refusing it here rather than per path means no path has to be trusted with the rule.
 */
public final class Statements {

    /**
     * The words a read-only console refuses.
     *
     * <p>Matched on word boundaries: a substring test would refuse {@code SELECT created_at} for
     * holding CREATE.
     */
    public static final List<String> WRITE_KEYWORDS = List.of(
            "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE", "GRANT", "REVOKE");

    private static final Pattern WRITE_KEYWORD = Pattern.compile(
            "\\b(" + String.join("|", WRITE_KEYWORDS) + ")\\b", Pattern.CASE_INSENSITIVE);

    private Statements() {}

    /** A statement the console refused, and why, in the words the page shows. */
    public static class Refused extends RuntimeException {

        private static final long serialVersionUID = 1L;

        Refused(String detail) {
            super(detail);
        }
    }

    /**
     * The statement as the engines will see it, or a refusal.
     *
     * <p>One trailing semicolon is taken off rather than refused, because a statement pasted from
     * {@code cqlsh} carries one and every engine here would refuse it. A second semicolon anywhere
     * is what a caller sending two statements looks like, and that is refused.
     *
     * @throws Refused if the statement is empty, is more than one, does not read, or names a word
     *     that writes
     */
    public static String validate(String sql) {
        String statement = sql == null ? "" : sql.strip();
        while (statement.endsWith(";")) {
            statement = statement.substring(0, statement.length() - 1).strip();
        }
        if (statement.isEmpty()) {
            throw new Refused("Empty query");
        }
        if (statement.indexOf(';') >= 0) {
            throw new Refused("Only a single statement is allowed");
        }
        if (!statement.toUpperCase(Locale.ROOT).startsWith("SELECT")) {
            throw new Refused("Only SELECT queries are allowed");
        }
        Matcher forbidden = WRITE_KEYWORD.matcher(statement);
        if (forbidden.find()) {
            throw new Refused("Forbidden keyword in a read-only console: "
                    + forbidden.group(1).toUpperCase(Locale.ROOT));
        }
        return statement;
    }
}
