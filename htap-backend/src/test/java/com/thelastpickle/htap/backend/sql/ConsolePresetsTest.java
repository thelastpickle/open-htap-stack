package com.thelastpickle.htap.backend.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.api.dto.SqlPreset;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * The presets, and the four defects they are written around.
 *
 * <p>Each test here is a defect stated as a rule, so a preset edited back into the shape that
 * reports a wrong number fails rather than shipping.
 */
class ConsolePresetsTest {

    /** {@code a.b * c.d}: arithmetic across two joined tables, which answers one operand. */
    private static final Pattern CROSS_TABLE_ARITHMETIC =
            Pattern.compile("\\w+\\.\\w+\\s*[*+/-]\\s*\\w+\\.\\w+");

    @Test
    void theEightPresetsAreDistinctAndComplete() {
        List<SqlPreset> presets = ConsolePresets.ALL;
        assertEquals(8, presets.size());
        assertEquals(
                List.of("transaction", "rollback", "join", "aggregate", "subquery", "having",
                        "oversubscribe", "explain"),
                presets.stream().map(SqlPreset::id).toList());
        for (SqlPreset preset : presets) {
            assertFalse(preset.title().isBlank(), preset.id());
            assertFalse(preset.description().isBlank(), preset.id());
            assertTrue(preset.sql().strip().endsWith(";"), preset.id());
        }
    }

    /** No parameters anywhere: an integer bind returns no rows here and raises nothing. */
    @Test
    void noPresetBindsAParameter() {
        for (SqlPreset preset : ConsolePresets.ALL) {
            assertFalse(preset.sql().contains("?"), preset.id());
        }
    }

    /** {@code ORDER BY} is discarded on a grouped result, so no preset asks for both. */
    @Test
    void noGroupedPresetAsksForAnOrder() {
        for (SqlPreset preset : ConsolePresets.ALL) {
            if (preset.sql().contains("GROUP BY")) {
                assertFalse(preset.sql().contains("ORDER BY"), preset.id());
            }
        }
        SqlPreset ungrouped = preset("subquery");
        assertTrue(ungrouped.sql().contains("ORDER BY"), "the ungrouped control lost its order");
    }

    /**
     * No preset multiplies across a join, because this engine answers such an expression with the
     * right-hand operand and discards the operator.
     */
    @Test
    void noPresetDoesArithmeticAcrossTwoJoinedTables() {
        for (SqlPreset preset : ConsolePresets.ALL) {
            assertFalse(
                    CROSS_TABLE_ARITHMETIC.matcher(preset.sql()).find(),
                    preset.id() + " does arithmetic across two qualified columns");
        }
    }

    /**
     * The join preset omits the per-leg distance.
     *
     * <p>{@code distance_km} is a column of both joined tables, and a name held by two of them
     * resolves to one table for the whole statement, so asking for the leg's answers the flight's.
     */
    @Test
    void theJoinPresetProjectsNoColumnNameItsJoinWouldResolveWrongly() {
        SqlPreset join = preset("join");
        assertTrue(join.sql().contains("INNER JOIN flights f"));
        assertTrue(join.sql().contains("FROM flight_legs l"));
        assertFalse(join.sql().contains("distance_km"), "the join preset asks for an ambiguous name");
        assertTrue(join.description().contains("distance_km"), "the omission is not explained");
    }

    /** The transaction is one string, because the engine executes a whole string as one unit. */
    @Test
    void theTransactionPresetOpensAndCommitsInOneStatement() {
        String sql = preset("transaction").sql();
        assertTrue(sql.startsWith("BEGIN;"), sql);
        assertTrue(sql.strip().endsWith("COMMIT;"), sql);
    }

    /**
     * The rollback preset ends in the {@code SELECT} that proves the write was held.
     *
     * <p>That closing statement is what the caller must be shown, which is why the client walks a
     * multi-statement string to its last result rather than reporting the first.
     */
    @Test
    void theRollbackPresetEndsInTheSelectThatProvesIt() {
        String sql = preset("rollback").sql();
        assertTrue(sql.contains("ROLLBACK;"), sql);
        assertTrue(sql.indexOf("ROLLBACK;") < sql.lastIndexOf("SELECT "), sql);
        assertTrue(sql.strip().endsWith("WHERE flight_id = 9099;"), sql);
    }

    private static SqlPreset preset(String id) {
        return ConsolePresets.ALL.stream()
                .filter(preset -> preset.id().equals(id))
                .findFirst()
                .orElseThrow();
    }
}
