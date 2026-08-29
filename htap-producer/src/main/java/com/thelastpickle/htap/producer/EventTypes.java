package com.thelastpickle.htap.producer;

import java.util.List;

/**
 * The twenty event types a reading may carry.
 *
 * <p>Deliberately generic rather than drone-specific: the schema is IoT telemetry, and the
 * fleet's naming lives in the dashboard's copy. An asset keeps one type for the life of the
 * process, since the type is taken from its index, which is what lets the compare page group
 * by {@code event_type} and get a stable answer.
 */
final class EventTypes {

    /** In the Python's order, because the index into it is what an asset's type comes from. */
    static final List<String> ALL = List.of(
            "telemetry_update",
            "position_report",
            "temperature_reading",
            "status_check",
            "health_monitor",
            "sensor_data",
            "diagnostic_report",
            "performance_metric",
            "environmental_scan",
            "system_heartbeat",
            "operational_status",
            "maintenance_alert",
            "calibration_check",
            "power_status",
            "connectivity_test",
            "data_sync",
            "threshold_check",
            "routine_inspection",
            "compliance_report",
            "activity_log");

    private EventTypes() {}

    /** The type asset {@code index} reports, which never changes for that asset. */
    static String of(int index) {
        return ALL.get(Math.floorMod(index, ALL.size()));
    }
}
