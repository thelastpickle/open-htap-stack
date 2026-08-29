package com.thelastpickle.htap.backend.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * The rule-based translator, which is what answers with no API key and no network.
 *
 * <p>Every statement it produces has to be one the console would run, so each case here is checked
 * against {@link Statements#validate} as well as against its own text: a rule that produced something
 * the validator refuses would turn a question into a 200 carrying a refusal.
 */
class NaturalLanguageTest {

    private static final String COLUMNS =
            "SELECT entity_id, event_time, latitude, longitude, altitude_m, speed_mps, "
                    + "temp_internal_c, risk_score FROM demo.drone_latest_status";

    /** A measure with no comparison is an ordering: the question asked which, not which above what. */
    @Test
    void aMeasureWithNoNumberOrdersByIt() {
        assertEquals(COLUMNS + " ORDER BY temp_internal_c DESC", sql("which drones are hottest"));
        assertEquals(COLUMNS + " ORDER BY altitude_m DESC", sql("highest altitude"));
        assertEquals(COLUMNS + " ORDER BY speed_mps DESC", sql("fastest drones"));
        assertEquals(COLUMNS + " ORDER BY risk_score DESC", sql("worst risk"));
    }

    @Test
    void anUpperBoundReadsAsGreaterThanAndOrdersDownwards() {
        assertEquals(COLUMNS + " WHERE temp_internal_c > 70 ORDER BY temp_internal_c DESC",
                sql("drones with temperature above 70"));
        assertEquals(COLUMNS + " WHERE speed_mps > 12.5 ORDER BY speed_mps DESC",
                sql("speed over 12.5"));
    }

    /** Ascending, because a question about what is below a bound is asking for the lowest first. */
    @Test
    void aLowerBoundReadsAsLessThanAndOrdersUpwards() {
        assertEquals(COLUMNS + " WHERE altitude_m < 30 ORDER BY altitude_m ASC",
                sql("altitude below 30"));
        assertEquals(COLUMNS + " WHERE risk_score < -1 ORDER BY risk_score ASC",
                sql("risk under -1"));
    }

    /** The second number is what tells a range from a bound, and it is read by the one pattern. */
    @Test
    void twoNumbersReadAsARange() {
        assertEquals(COLUMNS + " WHERE temp_internal_c BETWEEN 40 AND 60 "
                + "ORDER BY temp_internal_c DESC", sql("temperature between 40 and 60"));
        assertEquals(COLUMNS + " WHERE speed_mps BETWEEN 5 AND 9 ORDER BY speed_mps DESC",
                sql("speed between 5 to 9"));
    }

    /** "between" with one number is not a range, and reads as the bound it names. */
    @Test
    void betweenWithOneNumberIsNotARange() {
        assertEquals(COLUMNS + " WHERE temp_internal_c < 40 ORDER BY temp_internal_c ASC",
                sql("temperature between 40"));
    }

    @Test
    void theFlagQuestionsEachReadTheirOwnColumn() {
        assertEquals(COLUMNS + " WHERE predicted_zone_breach = true ORDER BY risk_score DESC",
                sql("which drones will breach"));
        assertEquals(COLUMNS + " WHERE near_restricted_zone = true ORDER BY risk_score DESC",
                sql("anything near a zone"));
        assertEquals(COLUMNS + " WHERE is_flying = false", sql("what is on the ground"));
        assertEquals(COLUMNS + " WHERE is_flying = true", sql("what is airborne"));
    }

    /** A measure is read before a flag, so a question naming both asks about the measure. */
    @Test
    void aMeasureIsReadBeforeAFlag() {
        assertEquals(COLUMNS + " ORDER BY risk_score DESC", sql("risk near a zone"));
    }

    @Test
    void aCountingQuestionAggregatesRatherThanListing() {
        assertEquals(
                "SELECT count(*) AS assets, count_if(is_flying) AS flying, "
                        + "count_if(near_restricted_zone) AS near_zone, "
                        + "round(avg(speed_mps), 1) AS avg_speed_mps FROM demo.drone_latest_status",
                sql("how many drones"));
    }

    /** A question no rule recognises still answers rows, since the page must render something. */
    @Test
    void anUnrecognisedQuestionListsTheFleet() {
        assertEquals(COLUMNS, sql("tell me about the fleet"));
        assertEquals(COLUMNS, sql(""));
    }

    @Test
    void theSpellingOfTheQuestionDoesNotMatter() {
        assertEquals(sql("temperature above 70"), sql("  TEMPERATURE Above 70  "));
    }

    /**
     * Read from the question and not from the rows, because the same columns are a table or a map
     * depending on what was asked.
     */
    @Test
    void theRenderHintComesFromTheQuestion() {
        assertEquals("map", NaturalLanguage.renderHint("where are the drones"));
        assertEquals("kpi", NaturalLanguage.renderHint("how many are flying"));
        assertEquals("chart", NaturalLanguage.renderHint("temperature over time"));
        assertEquals("table", NaturalLanguage.renderHint("list the drones"));
    }

    /** A map is asked for before a count, so "how many are near where" draws a map. */
    @Test
    void aQuestionAskingBothIsDrawnAsAMap() {
        assertEquals("map", NaturalLanguage.renderHint("how many drones and where"));
    }

    /** Presto, because ordering, ranges and aggregates are what CQL cannot answer. */
    @Test
    void theStatementRunsOnPresto() {
        assertEquals("presto", NaturalLanguage.ENGINE);
    }

    /** The model is told one table, and it is the one the rules name. */
    @Test
    void theSchemaOfferedToAModelNamesTheOneTableTheRulesUse() {
        assertTrue(NaturalLanguage.SCHEMA.startsWith("demo.drone_latest_status("),
                NaturalLanguage.SCHEMA);
        assertTrue(NaturalLanguage.SCHEMA.contains("risk_score"), NaturalLanguage.SCHEMA);
    }

    /** Every rule's statement is one the console would run, so none turns into a refusal. */
    private static String sql(String prompt) {
        String generated = NaturalLanguage.toSql(prompt);
        assertEquals(generated, Statements.validate(generated));
        return generated;
    }
}
