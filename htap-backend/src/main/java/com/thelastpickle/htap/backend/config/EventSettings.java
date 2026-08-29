package com.thelastpickle.htap.backend.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * How {@code demo.events} is partitioned, which the sink and every reader have to agree on.
 *
 * <p>The primary key is {@code ((event_bucket, shard), event_id)}, so both figures are part
 * of the key and neither can be derived from a row. Declared once in compose and read here
 * and by the sink: the two disagreeing produces queries that match nothing rather than
 * queries that fail.
 */
@ConfigMapping(prefix = "event")
public interface EventSettings {

    /** The width of one {@code event_bucket} window, in minutes of UTC. */
    @WithDefault("15")
    int bucketMinutes();

    /** How many {@code shard} values one bucket is spread over. */
    @WithDefault("16")
    int shards();
}
