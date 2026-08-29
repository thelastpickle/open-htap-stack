package com.thelastpickle.htap.backend.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thelastpickle.htap.backend.sql.ConsoleQuirks.Quirk;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Each defect is a join defect, which is what the pairing has to keep true. */
class ConsoleQuirksTest {

    @Test
    void theFourDefectsAreNamedAndDistinct() {
        assertEquals(
                List.of("column-resolution", "grouped-order-by", "cross-table-arithmetic",
                        "literal-arithmetic-dropped"),
                ConsoleQuirks.ALL.stream().map(Quirk::id).toList());
        for (Quirk quirk : ConsoleQuirks.ALL) {
            assertFalse(quirk.title().isBlank(), quirk.id());
            assertFalse(quirk.summary().isBlank(), quirk.id());
            assertFalse(quirk.expected().isBlank(), quirk.id());
            assertNotEquals(quirk.probe(), quirk.control(), quirk.id());
        }
    }

    /**
     * A probe and its control are separate statements.
     *
     * <p>They cannot be one string: a multi-statement string answers with its last result only, which
     * is the same property the rollback preset depends on.
     */
    @Test
    void everyStatementIsOneStatement() {
        for (Quirk quirk : ConsoleQuirks.ALL) {
            assertEquals(1, semicolons(quirk.probe()), quirk.id() + " probe");
            assertEquals(1, semicolons(quirk.control()), quirk.id() + " control");
        }
    }

    /**
     * Three of the four probes join, and the fourth's defect is the sort rather than the join.
     *
     * <p>The grouped-order-by probe is single-table on purpose: the clause is discarded on any grouped
     * result, so a join would add a second cause to a statement that has one.
     */
    @Test
    void everyProbeIsTheSmallestStatementThatShowsIt() {
        for (Quirk quirk : ConsoleQuirks.ALL) {
            boolean joins = quirk.probe().contains("INNER JOIN");
            assertEquals(!quirk.id().equals("grouped-order-by"), joins, quirk.id());
        }
    }

    /**
     * The arithmetic controls do the same arithmetic on one table.
     *
     * <p>That is what places the defect in the join: a product of a column and a literal is exact
     * here, and the same product across two joined tables is not.
     */
    @Test
    void theArithmeticControlsAreSingleTable() {
        for (String id : List.of("cross-table-arithmetic", "literal-arithmetic-dropped")) {
            String control = quirk(id).control();
            assertTrue(control.contains("*"), id);
            assertFalse(control.contains("JOIN"), id);
        }
    }

    /** The grouped control keeps the order the grouped probe loses. */
    @Test
    void theSortControlSortsAnUngroupedSelect() {
        Quirk quirk = quirk("grouped-order-by");
        assertTrue(quirk.probe().contains("GROUP BY"));
        assertTrue(quirk.probe().contains("ORDER BY"));
        assertTrue(quirk.control().contains("ORDER BY"));
        assertFalse(quirk.control().contains("GROUP BY"));
    }

    private static int semicolons(String sql) {
        return (int) sql.chars().filter(character -> character == ';').count();
    }

    private static Quirk quirk(String id) {
        return ConsoleQuirks.ALL.stream()
                .filter(candidate -> candidate.id().equals(id))
                .findFirst()
                .orElseThrow();
    }
}
