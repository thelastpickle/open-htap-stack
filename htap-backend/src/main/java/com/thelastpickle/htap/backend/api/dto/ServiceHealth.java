package com.thelastpickle.htap.backend.api.dto;

/**
 * @param status {@code up}, {@code down} or {@code unknown}
 * @param endpoint where this backend looked, so a page can show the address that failed
 */
public record ServiceHealth(String name, String status, String endpoint) {

    public static final String UP = "up";
    public static final String DOWN = "down";
    public static final String UNKNOWN = "unknown";

    public boolean up() {
        return UP.equals(status);
    }
}
