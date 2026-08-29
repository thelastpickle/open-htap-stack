package com.thelastpickle.htap.backend.api.dto;

/**
 * What a question in words was translated into, and what that statement answered.
 *
 * <p>The statement is reported whether it ran or not, which is the point of the field: a viewer who
 * gets no rows should see the SQL that asked for them, since a translation that misread the question
 * is the likeliest reason and a refused statement is the second.
 *
 * @param error a statement this backend refused, or an engine's own refusal; the status stays 200,
 *     because the translation is the answer and the page shows it beside the reason
 * @param renderHint how the page draws the answer: table, map, chart or kpi
 */
public record NlQueryResponse(
        String generatedSql,
        String engine,
        SqlQueryResult result,
        String error,
        String renderHint) {

    /** A statement that never ran, with the reason it did not. */
    public static NlQueryResponse refused(String sql, String error, String renderHint) {
        return new NlQueryResponse(sql, null, null, error, renderHint);
    }
}
