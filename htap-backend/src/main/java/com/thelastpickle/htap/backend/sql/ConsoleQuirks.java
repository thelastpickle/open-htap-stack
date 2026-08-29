package com.thelastpickle.htap.backend.sql;

import java.util.List;

/**
 * The four join defects, each with the control that isolates it.
 *
 * <p>Shown rather than avoided. Two presets had been reporting a wrong number as though it were
 * right, and correcting them silently would have left the page implying that this engine answers a
 * join correctly. It does not, and the four ways it does not are reproducible in one statement each.
 *
 * <p>Each defect is paired with a control, because a defect is only a defect if something nearby
 * works: the same arithmetic on one table is exact, and {@code ORDER BY} on an ungrouped
 * {@code SELECT} is exact, so all four are join defects rather than arithmetic or sort defects.
 *
 * <p>A pair cannot be one SQL string. A multi-statement string answers with its last result only,
 * which is what makes the rollback preset work, so each probe and each control is sent on its own.
 *
 * <p>All four were measured at cassandra-sql revision
 * a0257ec9a22ff84daaf6f529ae8b523fdc45b431, which podman-compose pins, so nothing upstream can
 * change under this page without the pin changing. They are a reason to read the join presets
 * carefully rather than a reason to dismiss the engine, which is a proof of concept by its own
 * account.
 *
 * <p>Four more defects are constraints the SQL layer accepts and does not hold, and they are
 * recorded here rather than probed because reproducing one leaves a row behind. A duplicate primary
 * key upserts; a {@code FOREIGN KEY} is accepted and a flight naming operator 9999 and drone 9999 is
 * then stored although neither exists; {@code NOT NULL} is accepted and an operator with no name is
 * stored; and an {@code ENUM} is accepted and {@code status} stores the string 'nonsense'.
 * <b>{@code UNIQUE} is the one declared constraint that is held</b>, which is what makes the seed
 * non-idempotent and why a reset route exists at all. Worth stating the other way round too: the
 * layer <i>can</i> hold a constraint above the storage engine, so the foreign key going unenforced is
 * a gap in this prototype rather than an impossibility.
 */
final class ConsoleQuirks {

    /** One defect and the two statements that show it. */
    record Quirk(String id, String title, String summary, String expected, String probe,
            String control) {}

    private ConsoleQuirks() {}

    static final List<Quirk> ALL = List.of(
            new Quirk(
                    "column-resolution",
                    "A column name in two joined tables resolves to the wrong one",
                    """
                    distance_km is a column of both flights and flight_legs.  Asking for the leg's \
                    answers the flight's, for every leg, and the l. qualifier is ignored.  An alias \
                    does not help, and the second table named is the one that wins.  Dropping the \
                    flights join is what makes the same projection answer correctly.""",
                    "6.2 and 15.2, the two legs' own distances",
                    "SELECT l.leg_no, l.distance_km FROM flight_legs l "
                            + "INNER JOIN flights f ON l.flight_id = f.flight_id;",
                    "SELECT l.leg_no, l.distance_km FROM flight_legs l "
                            + "INNER JOIN zones z ON l.zone_id = z.zone_id;"),
            new Quirk(
                    "grouped-order-by",
                    "ORDER BY is ignored on a grouped result",
                    """
                    The clause is accepted and discarded.  ORDER BY n DESC answers the same order \
                    as ORDER BY airframe, so it is not that one key is mishandled: the sort does \
                    not run at all.  The same clause on an ungrouped SELECT is exact.""",
                    "fixed-wing, octocopter, quadcopter, vtol",
                    "SELECT airframe, COUNT(*) AS n FROM drones GROUP BY airframe ORDER BY airframe;",
                    "SELECT serial, mass_kg FROM drones ORDER BY mass_kg DESC;"),
            // The control multiplies by a literal 45.0 rather than by each zone's own fee, because
            // reaching the fee is exactly what needs the join. So its numbers are not the expected
            // ones: 540.0 and 405.0 at one flat rate, where the join should answer 540.0 for
            // Gardermoen at 45 and 162.0 for Fornebu at 18. What it shows is that the multiplication
            // itself is exact, which is what makes the join the thing at fault.
            new Quirk(
                    "cross-table-arithmetic",
                    "Arithmetic across two joined tables returns one operand",
                    """
                    The operator is discarded and the right-hand operand is returned.  Reverse the \
                    operands and the other column comes back; write + instead of * and nothing \
                    changes.  This is what made a fee-per-minute total read as the fee.""",
                    """
                    540.0 for the Gardermoen leg at 45 NOK/min and 162.0 for the Fornebu leg at 18; \
                    the probe answers the two fees instead""",
                    "SELECT l.leg_no, l.dwell_min * z.fee_per_min AS fee_nok FROM flight_legs l "
                            + "INNER JOIN zones z ON l.zone_id = z.zone_id;",
                    "SELECT leg_no, dwell_min * 45.0 AS fee_at_one_flat_rate FROM flight_legs;"),
            new Quirk(
                    "literal-arithmetic-dropped",
                    "Arithmetic against a literal inside a join drops the column",
                    """
                    The column is absent from the result, not merely wrong, so a client reading by \
                    position silently reads the next column instead.  This is the worst of the four \
                    for that reason: the other three answer something, and this one answers a \
                    differently shaped row.""",
                    "three columns: leg_no, times_one, capacity",
                    "SELECT l.leg_no, l.dwell_min * 1.0 AS times_one, z.capacity FROM flight_legs l "
                            + "INNER JOIN zones z ON l.zone_id = z.zone_id;",
                    "SELECT leg_no, dwell_min * 1.0 AS times_one FROM flight_legs;"));
}
