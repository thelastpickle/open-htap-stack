package com.thelastpickle.htap.backend.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.Optional;

/**
 * Where the CQL request path connects, from the environment compose sets.
 *
 * <p>A method name maps to the environment variable the Python read: {@code host} to
 * {@code CASSANDRA_HOST}, {@code translateAddressesTo} to
 * {@code CASSANDRA_TRANSLATE_ADDRESSES_TO}, and so on for the rest.
 */
@ConfigMapping(prefix = "cassandra")
public interface CassandraSettings {

    @WithDefault("cassandra")
    String host();

    @WithDefault("9042")
    int port();

    @WithDefault("demo")
    String keyspace();

    /** Named in the bulk reader's options too; the driver addresses nodes by datacenter. */
    @WithDefault("datacenter1")
    String datacenter();

    /**
     * The Sidecar's HTTP port, on the same host as the node.
     *
     * <p>Two access paths reach it: the bulk reader takes its snapshot through the Sidecar,
     * and the backend asks the same service how large that snapshot was. Under
     * {@code cassandra} rather than a prefix of its own, because a Sidecar serves one node
     * and its address is the node's.
     */
    @WithDefault("9043")
    int sidecarPort();

    /**
     * The address to rewrite every discovered node address to, absent for no translation.
     *
     * <p>A backend run on the host rather than inside the compose network is given
     * {@code 127.0.0.1}: the driver otherwise discovers the node's broadcast address,
     * {@code 172.20.0.10}, and cannot reach it. No translation is correct in-network.
     *
     * <p>Optional rather than an empty default, because SmallRye converts an empty string to
     * null and then refuses it for a non-optional property. Compose passing the variable
     * through unset therefore has to arrive as an absent value rather than as {@code ""}.
     */
    Optional<String> translateAddressesTo();
}
