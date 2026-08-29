package com.thelastpickle.htap.cqlite;

import java.time.Duration;
import java.util.Optional;

/**
 * What a registered table's directory holds now, read without opening an SSTable.
 *
 * @param files how many SSTable files the directory holds
 * @param bytes their total size
 * @param dataAge how long ago the newest of them was written, empty when the boundary
 *     reported no age
 */
public record Discovery(long files, long bytes, Optional<Duration> dataAge) {

    /**
     * Reads the struct's three fields, turning the boundary's {@code -1} into an empty
     * age.
     *
     * <p>A directory holding no file has no newest file, so the absence is ordinary and
     * is carried as such rather than as a sentinel a caller might report as a figure.
     */
    static Discovery of(long files, long bytes, long ageSeconds) {
        return new Discovery(files, bytes, age(ageSeconds));
    }

    /**
     * Any negative, and not only {@link Abi#NO_AGE}, because a clock that moved
     * backwards would otherwise be reported as an age.
     */
    static Optional<Duration> age(long ageSeconds) {
        return ageSeconds < 0L
                ? Optional.empty()
                : Optional.of(Duration.ofSeconds(ageSeconds));
    }
}
