package com.thelastpickle.htap.common;

/**
 * The one distance the sink and the dashboard both measure a position against.
 *
 * <p>Here rather than in either of them because both read it and the two must agree: the sink writes
 * an alert when an asset comes within this distance of a restricted zone, and the dashboard's what-if
 * simulation counts the assets a hypothetical zone would have nearby. Two copies of the figure would
 * let the page disagree with the table it is drawn beside, and the disagreement would look like a
 * data fault rather than a constant that drifted.
 *
 * <p>Named for the rule and not for the zones, because the sink has a {@code Zones} of its own that
 * reads the table; two types of that name in one file would read as the same thing.
 */
public final class ZoneRules {

    /** Distance from a zone's boundary at which proximity is reported at all. */
    public static final double WARNING_DISTANCE_M = 500.0;

    private ZoneRules() {}
}
