package com.thelastpickle.htap.backend.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.nio.file.Path;

/**
 * The SSTable files the in-process reader opens, and how it is told to read them.
 *
 * <p>Every default here was measured; see the notes in {@code CLAUDE.md} before changing one.
 */
@ConfigMapping(prefix = "cqlite")
public interface CqliteSettings {

    /**
     * The node's data directory, mounted read-only into this container.
     *
     * <p>The reader opens the files where they lie, so this is the same directory Cassandra is
     * writing: one keyspace directory below it per keyspace, and one table directory below
     * that.
     */
    @WithDefault("/var/lib/cassandra/data")
    Path dataDir();

    /**
     * The shared library the binding loads.
     *
     * <p>The image gunzips it here from {@code htap-cqlite/dist/}. A path that does not exist
     * is not a failure: the access path reports itself unavailable, which is what a backend
     * run on the host does.
     */
    @WithDefault("/opt/htap/lib/libcqlite_datafusion_c.so")
    Path library();

    /**
     * How many token slices one statement is divided into.
     *
     * <p>1, because most of a slice is work every other slice repeats: the route this stack's
     * files take drains the data section sequentially with no partition-index seek, so each
     * slice re-reads and re-parses the whole file and only the row decode divides. Measured at
     * 1, 2, 4 and 7 slices on both SSTable versions, and N times the processor time bought no
     * wall clock either time.
     */
    @WithDefault("1")
    long splits();

    /** Rows per Arrow batch the reader hands back. */
    @WithDefault("8192")
    long batchRows();

    /**
     * How many named partitions go to one merger at a time.
     *
     * <p>1, because this is what bounds the memory: the seek merger decodes every row of every
     * partition it is given before the merge starts, so a window of 16 partitions read
     * together held 4.84 GB of anonymous memory against 1.09 GB read one at a time, and one at
     * a time was also faster.
     */
    @WithDefault("1")
    long keyChunk();
}
