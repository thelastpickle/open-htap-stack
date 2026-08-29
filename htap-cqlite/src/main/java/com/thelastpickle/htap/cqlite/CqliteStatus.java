package com.thelastpickle.htap.cqlite;

/**
 * What the C boundary answered: {@link #OK}, or the reason it refused.
 *
 * <p>The codes are the header's, and {@code CqliteAbiTest} checks each one against
 * the committed {@code cqlite_datafusion.h} rather than trusting this file.
 *
 * <p>The distinction the dashboard acts on is {@link #NOT_READY} against the rest.
 * A table Cassandra has not flushed has no file to read, and that state passes on
 * its own as soon as a flush happens, so the path reports a decline; every other
 * code is a failure a viewer cannot wait out.
 */
public enum CqliteStatus {

    /** The call succeeded. */
    OK(0),

    /** The call failed for a reason none of the other codes names. */
    ERROR(-1),

    /**
     * The files are not there to be read yet: a missing directory, a path that is
     * not a directory, or a table Cassandra has not flushed.
     */
    NOT_READY(-2),

    /**
     * The {@code CREATE TABLE} statement does not parse, or names a CQL type the
     * reader cannot map to Arrow.
     */
    SCHEMA(-3),

    /** An argument was null, was not UTF-8, or did not fit the type it names. */
    BAD_ARGUMENT(-4),

    /** A panic was caught at the boundary, which is a defect in the library. */
    PANIC(-5);

    private final int code;

    CqliteStatus(int code) {
        this.code = code;
    }

    /** The integer the boundary returns for this status. */
    public int code() {
        return code;
    }

    /**
     * The status a returned code names, or {@link #ERROR} for a code this binding
     * does not know.
     *
     * <p>An unknown code can only come from a library whose application binary interface
     * (ABI) version matched while its codes did not, so it is not thrown on: {@link
     * CqliteException} keeps the number the boundary actually returned beside the status.
     */
    public static CqliteStatus of(int code) {
        for (CqliteStatus status : values()) {
            if (status.code == code) {
                return status;
            }
        }
        return ERROR;
    }
}
