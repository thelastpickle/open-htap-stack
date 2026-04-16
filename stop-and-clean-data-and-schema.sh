#!/usr/bin/env bash
set -euo pipefail

echo "Stopping containers (and cleaning Kafka data)..."
podman compose -f podman-compose.yml down -v
echo "Cleaning Cassandra and Parquet schema and data..."
rm -rf cassandra-data/*/*
