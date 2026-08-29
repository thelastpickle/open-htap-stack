package com.thelastpickle.htap.backend.api.dto;

/**
 * One column, as the engine that owns it describes it.
 *
 * @param kind {@code partition_key}, {@code clustering}, {@code regular} or {@code static} on the
 *     CQL side
 * @param position the position within the partition key or the clustering key, -1 for neither
 * @param clusteringOrder {@code asc} or {@code desc} for a clustering column, {@code none}
 *     otherwise
 */
public record SchemaColumn(
        String name, String type, String kind, int position, String clusteringOrder) {}
