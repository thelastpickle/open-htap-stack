# Presto with Cassandra Integration

This setup configures PrestoDB to query Cassandra data using the built-in Cassandra connector.

## Configuration

The Cassandra catalog is configured in `presto/etc/catalog/cassandra.properties`:

```properties
connector.name=cassandra
cassandra.contact-points=cassandra:9042
cassandra.load-policy.dc-aware.local-dc=datacenter1
cassandra.username=cassandra
cassandra.password=cassandra
cassandra.protocol-version=V5
cassandra.allow-drop-table=true
cassandra.native-protocol-port=9042
```

## Accessing Presto

### Web UI
Access the Presto Web UI at: http://localhost:8088

### CLI Access
Connect to Presto CLI from within the container:
```bash
podman exec -it presto presto-cli
```

Or from your host (if presto-cli is installed):
```bash
presto --server localhost:8088 --catalog cassandra
```

## Example Queries

### List Available Catalogs
```sql
SHOW CATALOGS;
```

### List Cassandra Keyspaces
```sql
SHOW SCHEMAS FROM cassandra;
```

### List Tables in a Keyspace
```sql
SHOW TABLES FROM cassandra.demo;
```

### Describe Table Structure
```sql
DESCRIBE cassandra.demo.events;
```

### Query Data from Cassandra
```sql
-- Select all data from the demo.events table
SELECT * FROM cassandra.demo.events LIMIT 100;

-- Count records
SELECT COUNT(*) FROM cassandra.demo.events;

-- Filter and aggregate
SELECT 
    event_type,
    COUNT(*) as count,
    MIN(event_time) as first_event,
    MAX(event_time) as last_event
FROM cassandra.demo.events
GROUP BY event_type
ORDER BY count DESC;
```

### Join with Other Data Sources
Presto allows you to join Cassandra data with other catalogs (if configured):
```sql
-- Example: Join Cassandra with system information
SELECT 
    c.event_id,
    c.event_type,
    c.event_time
FROM cassandra.demo.events c
WHERE c.event_time > CURRENT_TIMESTAMP - INTERVAL '1' HOUR
LIMIT 100;
```

## Performance Tips

1. **Use partition key filters**: Always filter by partition keys when possible for better performance
2. **Limit result sets**: Use LIMIT clause to avoid scanning large datasets
3. **Avoid SELECT ***: Specify only the columns you need
4. **Use EXPLAIN**: Check query execution plans with `EXPLAIN` or `EXPLAIN ANALYZE`

Example with partition key filter:
```sql
-- Assuming partition_key is a partition key column
SELECT * FROM cassandra.demo.events 
WHERE partition_key = 'some_value'
LIMIT 100;
```

## Troubleshooting

### Check Presto Logs
```bash
podman logs presto
```

### Verify Cassandra Connection
```bash
# From within the Presto container
podman exec -it presto nc -zv cassandra 9042
```

### Check Catalog Configuration
```bash
podman exec -it presto cat /opt/presto-server/etc/catalog/cassandra.properties
```

### Restart Presto
If you make configuration changes:
```bash
podman compose -f podman-compose.yml restart presto
```

## Limitations

- The Cassandra connector in PrestoDB has some limitations with certain Cassandra data types
- Complex CQL queries (like collections, UDTs) may have limited support
- Write operations are limited (INSERT/DELETE support varies by version)
- For best performance, design your Cassandra schema with Presto queries in mind

## Additional Resources

- [PrestoDB Cassandra Connector Documentation](https://prestodb.io/docs/current/connector/cassandra.html)
- [Cassandra Data Modeling Best Practices](https://cassandra.apache.org/doc/latest/data_modeling/)