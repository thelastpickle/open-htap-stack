Place your custom PrestoDB connector plugin JAR(s) for Sidecar/SSTable streaming here.

Expected container path:
  /opt/presto-server/plugin/sstable

After adding jars, restart:
  podman compose -f podman-compose.yml restart presto
